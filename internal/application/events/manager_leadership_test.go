package events

import (
	"net/http"
	"net/http/httptest"
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
			Http: &commonpb.HttpSinkConfig{Endpoint: connectionTestURL(t, server.URL)},
		},
	})

	m := NewManager(store, attrs, nil, builder, logging.Testing(), notifications)
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
