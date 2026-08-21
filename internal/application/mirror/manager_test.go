package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
func mirrorLedgerInfo(name, baseURL string) *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name: name,
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		MirrorSource: &commonpb.MirrorSourceConfig{
			LedgerName: "source-ledger",
			Type: &commonpb.MirrorSourceConfig_Http{
				Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: baseURL},
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
	t.Cleanup(func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.teardown()
	})

	return m
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
	saveLedgerInfo(t, store, mirrorLedgerInfo("mirrored", quietV2Source(t)))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)

	require.Equal(t, []string{"mirrored"}, workerNames(m))
}

// Promotion flips the ledger out of MIRROR mode, so ReadMirrorLedgers stops
// listing it and reconcile must drop its worker. Until it does, the worker keeps
// polling the v2 source and proposing ingests that the FSM rejects with
// ErrLedgerNotInMirrorMode.
func TestManager_ReconcileStopsWorkerOnPromotion(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	sourceURL := quietV2Source(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo("promoted", sourceURL))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	require.Equal(t, []string{"promoted"}, workerNames(m))

	// Promote: mode back to NORMAL and the source config cleared, matching
	// processPromoteLedger.
	saveLedgerInfo(t, store, &commonpb.LedgerInfo{
		Name: "promoted",
		Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
	})

	m.OnLeadershipChange(true)
	require.Empty(t, workerNames(m), "a promoted ledger is no longer a mirror; its worker must be stopped and dropped")
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
	saveLedgerInfo(t, store, mirrorLedgerInfo("deleted", sourceURL))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	require.Equal(t, []string{"deleted"}, workerNames(m))

	info := mirrorLedgerInfo("deleted", sourceURL)
	info.DeletedAt = &commonpb.Timestamp{Data: 1}
	saveLedgerInfo(t, store, info)

	m.OnLeadershipChange(true)
	require.Empty(t, workerNames(m), "a deleted ledger must not keep a worker, even though it is still in MIRROR mode")
}

// Losing leadership tears every worker down, whatever the config says: workers
// only run on the leader.
func TestManager_ReconcileTearsDownOnLeadershipLoss(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo("mirrored", quietV2Source(t)))

	m := newTestManager(t, store, builder)
	m.OnLeadershipChange(true)
	require.Equal(t, []string{"mirrored"}, workerNames(m))

	m.OnLeadershipChange(false)
	require.Empty(t, workerNames(m))

	// And regaining leadership rebuilds the set from the store.
	m.OnLeadershipChange(true)
	require.Equal(t, []string{"mirrored"}, workerNames(m))
}
