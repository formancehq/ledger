package events

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func managedSinkByName(m *Manager, name string) *managedSink {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.emitters[name]
}

func saveManagedSinkConfig(t *testing.T, attrs *attributes.Attributes, store *dal.Store, config *commonpb.SinkConfig) {
	t.Helper()

	batch := store.OpenWriteSession()
	_, err := attrs.SinkConfig.Set(batch, domain.SinkConfigKey{Name: config.GetName()}.Bytes(), config)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func TestManager_StopFencesLaterLeadershipGain(t *testing.T) {
	t.Parallel()

	m := &Manager{
		notifications: signal.NewNotifications(),
		emitters:      make(map[string]*managedSink),
		retries:       make(map[string]struct{}),
	}
	m.Start()
	m.Stop()

	m.OnLeadershipChange(true)

	_, isLeader, stopped := m.leadershipSnapshot()
	require.True(t, stopped)
	require.False(t, isLeader)
	require.Empty(t, m.emitters, "a leadership callback after Stop must not recreate event emitters")
}

func TestManager_LeadershipChangeCancelsStartupProposals(t *testing.T) {
	t.Parallel()
	m := &Manager{notifications: signal.NewNotifications()}
	m.OnLeadershipChange(true)
	generation, _, _ := m.leadershipSnapshot()
	ctx, current := m.currentLeaderContext(generation)
	require.True(t, current)
	m.OnLeadershipChange(false)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	_, current = m.currentLeaderContext(generation)
	require.False(t, current)
}

func TestManager_SupersededLossCannotTearDownCurrentGeneration(t *testing.T) {
	t.Parallel()

	m := &Manager{
		notifications: signal.NewNotifications(),
		emitters:      map[string]*managedSink{"current": nil},
		retries:       make(map[string]struct{}),
	}

	m.OnLeadershipChange(false)
	staleGeneration, staleIsLeader, staleStopped := m.leadershipSnapshot()
	m.OnLeadershipChange(true)

	m.mu.Lock()
	m.reconcileGeneration(staleGeneration, staleIsLeader, staleStopped)
	m.mu.Unlock()

	require.Contains(t, m.emitters, "current", "a superseded leadership loss must not tear down current-generation event emitters")
}

func TestManager_CoalescedLeadershipFlapReplacesPriorGenerationEmitter(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	notifications := signal.NewNotifications()
	saveManagedSinkConfig(t, attrs, store, &commonpb.SinkConfig{
		Name: "http-sink",
		Type: &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{Endpoint: server.URL},
		},
	})

	m := NewManager(store, attrs, &startupStatusProposer{store: store}, builder, logging.Testing(), notifications)
	m.Start()
	t.Cleanup(m.Stop)
	m.OnLeadershipChange(true)

	require.Eventually(t, func() bool {
		return managedSinkByName(m, "http-sink") != nil
	}, time.Second, 10*time.Millisecond)
	previous := managedSinkByName(m, "http-sink")

	func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		// Occupy the lifecycle loop on the manager lock, then enqueue a complete
		// leadership flap. Only one config notification can remain buffered.
		notifications.NotifyLogsCommitted(1)
		require.Eventually(t, func() bool {
			return len(notifications.LogCommitted.C()) == 0
		}, time.Second, 10*time.Millisecond, "the lifecycle loop must consume the log notification before the flap")

		m.OnLeadershipChange(false)
		m.OnLeadershipChange(true)
		require.Len(t, notifications.ConfigChanged.C(), 1, "the loss and regain notifications must coalesce")
	}()

	require.Eventually(t, func() bool {
		current := managedSinkByName(m, "http-sink")

		return current != nil && current != previous
	}, time.Second, 10*time.Millisecond, "an emitter from before the loss must not be retained by the new leader generation")
}

type testLeadershipLease struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

func newTestLeadershipLease() *testLeadershipLease {
	ctx, cancel := context.WithCancel(context.Background())

	return &testLeadershipLease{ctx: ctx, cancel: cancel}
}

func (l *testLeadershipLease) Context() context.Context { return l.ctx }
func (l *testLeadershipLease) Epoch() uint64            { return 1 }
func (l *testLeadershipLease) Authorize(fn func()) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ctx.Err() != nil {
		return false
	}
	fn()

	return true
}
func (l *testLeadershipLease) Revoke() { l.mu.Lock(); l.cancel(); l.mu.Unlock() }

func TestManager_LeaseRevocationFencesStartup(t *testing.T) {
	t.Parallel()
	m := &Manager{notifications: signal.NewNotifications()}
	lease := newTestLeadershipLease()
	m.OnLeadershipLease(lease)
	generation, _, _ := m.leadershipSnapshot()
	ctx, current := m.currentLeaderContext(generation)
	require.True(t, current)
	lease.Revoke()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.False(t, m.isCurrentLeader(generation))
}

func TestManager_RevokedLeaseCannotStartPreparedSink(t *testing.T) {
	t.Parallel()
	m := &Manager{notifications: signal.NewNotifications(), emitters: make(map[string]*managedSink)}
	lease := newTestLeadershipLease()
	m.OnLeadershipLease(lease)
	generation, _, _ := m.leadershipSnapshot()
	ctx, current := m.currentLeaderContext(generation)
	require.True(t, current)
	prepared := &managedSink{emitter: &Emitter{}}
	lease.Revoke()
	require.False(t, m.registerSink(generation, "prepared", prepared, ctx))
	require.Empty(t, m.emitters)
	require.Nil(t, prepared.emitter.stopCh, "revoked lease must not launch emitter")
}
