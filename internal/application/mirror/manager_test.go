package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// quietV2Source serves an empty log page so a worker started by reconcile parks
// on its poll interval instead of erroring into backoff. reconcile builds its
// own source through createSource, so a v2.MockSource cannot be injected —
// the worker set itself is what these tests assert on.
func quietV2Source(t *testing.T) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v2.V2LogPage{
			Cursor: v2.V2LogCursor{PageSize: 10, HasMore: false},
		})
	}))
	t.Cleanup(srv.Close)

	return srv.URL
}

// mirrorLedgerInfo builds a LedgerInfo for a mirror ledger sourced from baseURL.
func mirrorLedgerInfo(t *testing.T, name, baseURL string) *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name: name,
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		MirrorSource: &commonpb.MirrorSourceConfig{
			LedgerName: "source-ledger",
			Type: &commonpb.MirrorSourceConfig_Http{
				Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: connectionTestURL(t, baseURL)},
			},
		},
	}
}

// saveLedgerInfo persists a LedgerInfo where reconcile looks for it. Note this
// is ZoneGlobal/SubGlobLedgerInfo (what query.ReadLedgers scans), NOT the
// SubAttrLedger attribute row.
func saveLedgerInfo(t *testing.T, store *dal.Store, info *commonpb.LedgerInfo) {
	t.Helper()

	session := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(session, info.GetName(), info))
	require.NoError(t, session.Commit())
}

// errProposer stands in for the Raft proposer. Unlike newWorkerForTest, these
// tests run real worker loops: reconcile calls Worker.Start, and a worker
// cancelled mid-fetch by teardown returns an error from processBatch, which
// processLogs hands to reportError — and that proposes through
// plan.Builder.Run, where a nil Proposer is dereferenced without a guard
// (runner.go). Returning an error keeps reportError on its logging path.
type errProposer struct{}

func (errProposer) Propose(context.Context, *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	return nil, errors.New("no proposer in manager tests")
}

func newTestManager(t *testing.T, store *dal.Store, builder *plan.Builder) *Manager {
	t.Helper()

	m := NewManager(
		store, errProposer{}, builder,
		logging.FromContext(logging.TestingContext()),
		signal.NewNotifications(),
		noop.NewMeterProvider(),
		0,
	)
	m.Start()
	t.Cleanup(m.Stop)

	return m
}

func requireWorkerNames(t *testing.T, m *Manager, expected ...string) {
	t.Helper()

	require.Eventually(t, func() bool {
		actual := workerNames(m)
		if len(actual) != len(expected) {
			return false
		}

		for _, expectedName := range expected {
			found := slices.Contains(actual, expectedName)
			if !found {
				return false
			}
		}

		return true
	}, time.Second, 10*time.Millisecond)
}

// workerNames snapshots the live worker set under the manager's lock.
func workerNames(m *Manager) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0, len(m.workers))
	for name := range m.workers {
		names = append(names, name)
	}

	return names
}

// A mirror ledger gets a worker once the node is leader. This is the baseline
// the removal cases below measure against.
func TestManager_ReconcileStartsWorkerForMirrorLedger(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo(t, "mirrored", quietV2Source(t)))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)

	requireWorkerNames(t, m, "mirrored")
}

// Promotion flips the ledger out of MIRROR mode, so ReadMirrorLedgers stops
// listing it and reconcile must drop its worker. Until it does, the worker keeps
// polling the v2 source and proposing ingests that the FSM rejects with
// ErrLedgerNotInMirrorMode.
func TestManager_ReconcileStopsWorkerOnPromotion(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	sourceURL := quietV2Source(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo(t, "promoted", sourceURL))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	requireWorkerNames(t, m, "promoted")

	// Promote: mode back to NORMAL and the source config cleared, matching
	// processPromoteLedger.
	saveLedgerInfo(t, store, &commonpb.LedgerInfo{
		Name: "promoted",
		Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
	})

	m.OnLeadershipChange(true)
	requireWorkerNames(t, m)
}

// Deletion soft-deletes the ledger, which also drops it from ReadMirrorLedgers
// (it filters DeletedAt == nil), so the same removal branch must fire.
//
// This test drives reconcile directly. On a live cluster the trigger is
// WriteSet.Absorb's DeleteLedger case, which marks the mirror config as changed
// so a ConfigChanged notification reaches the manager — covered by
// write_set_absorb_test.go and end to end in the cluster suite.
func TestManager_ReconcileStopsWorkerOnDeletion(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	sourceURL := quietV2Source(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo(t, "deleted", sourceURL))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	requireWorkerNames(t, m, "deleted")

	info := mirrorLedgerInfo(t, "deleted", sourceURL)
	info.DeletedAt = &commonpb.Timestamp{Data: 1}
	saveLedgerInfo(t, store, info)

	m.OnLeadershipChange(true)
	requireWorkerNames(t, m)
}

// Losing leadership tears every worker down, whatever the config says: workers
// only run on the leader.
func TestManager_ReconcileTearsDownOnLeadershipLoss(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo(t, "mirrored", quietV2Source(t)))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	requireWorkerNames(t, m, "mirrored")

	m.OnLeadershipChange(false)
	requireWorkerNames(t, m)

	// And regaining leadership rebuilds the set from the store.
	m.OnLeadershipChange(true)
	requireWorkerNames(t, m, "mirrored")
}

func TestManager_StopFencesLaterLeadershipGain(t *testing.T) {
	t.Parallel()

	m := &Manager{
		notifications: signal.NewNotifications(),
		workers:       make(map[string]*Worker),
	}
	m.Start()
	m.Stop()

	m.OnLeadershipChange(true)

	leadership := m.leadershipSnapshot()
	require.True(t, leadership.stopped)
	require.False(t, leadership.isLeader)
	require.Empty(t, workerNames(m), "a leadership callback after Stop must not recreate mirror workers")
}

func TestManager_LeadershipChangesCancelInitialization(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	first := m.leadershipSnapshot()
	require.NoError(t, first.initializationCtx.Err())

	m.OnLeadershipChange(false)
	require.ErrorIs(t, first.initializationCtx.Err(), context.Canceled,
		"loss must cancel initialization before its callback returns")

	m.OnLeadershipChange(true)
	second := m.leadershipSnapshot()
	require.Greater(t, second.generation, first.generation)
	require.NoError(t, second.initializationCtx.Err())

	m.OnLeadershipChange(true)
	third := m.leadershipSnapshot()
	require.ErrorIs(t, second.initializationCtx.Err(), context.Canceled,
		"every superseding generation must cancel prior initialization")
	require.NoError(t, third.initializationCtx.Err())

	m.Stop()
	require.ErrorIs(t, third.initializationCtx.Err(), context.Canceled)
	m.OnLeadershipChange(true)
	require.Equal(t, third.generation+1, m.leadershipSnapshot().generation,
		"late gain must not create a new generation after Stop")
}

func TestManager_SupersededLossCannotTearDownCurrentGeneration(t *testing.T) {
	t.Parallel()

	m := &Manager{
		notifications: signal.NewNotifications(),
		workers:       map[string]*Worker{"current": nil},
	}

	m.OnLeadershipChange(false)
	staleLeadership := m.leadershipSnapshot()
	m.OnLeadershipChange(true)

	m.mu.Lock()
	m.reconcileGeneration(staleLeadership)
	m.mu.Unlock()

	require.Contains(t, m.workers, "current", "a superseded leadership loss must not tear down current-generation mirror workers")
}

func TestManager_CoalescedLeadershipFlapReplacesPriorGenerationWorker(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo(t, "mirrored", quietV2Source(t)))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	requireWorkerNames(t, m, "mirrored")

	m.mu.Lock()
	previous := m.workers["mirrored"]
	m.mu.Unlock()

	func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		// Occupy the lifecycle loop on the manager lock, then enqueue a complete
		// leadership flap. Only one config notification can remain buffered.
		m.notifications.NotifyLogsCommitted(1)
		require.Eventually(t, func() bool {
			return len(m.notifications.LogCommitted.C()) == 0
		}, time.Second, 10*time.Millisecond, "the lifecycle loop must consume the log notification before the flap")

		m.OnLeadershipChange(false)
		m.OnLeadershipChange(true)
		require.Len(t, m.notifications.ConfigChanged.C(), 1, "the loss and regain notifications must coalesce")
	}()

	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()

		current := m.workers["mirrored"]

		return current != nil && current != previous
	}, time.Second, 10*time.Millisecond, "a worker from before the loss must not be retained by the new leader generation")
}
