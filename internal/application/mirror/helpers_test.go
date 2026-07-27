package mirror

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// newTestBuilder spins up a minimal plan.Builder against a temp Pebble store
// so worker tests can exercise the boundary-derived resume position. The
// store is closed on test cleanup.
func newTestBuilder(t *testing.T) (*plan.Builder, *dal.Store) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	testCache, err := cache.New(100, meter)
	require.NoError(t, err)

	return plan.NewBuilder(node.NewIndexTracker(1), testCache, attributes.New(), store, nil, logger, 0), store
}

// writeBoundaries persists a LedgerBoundaries row so a worker constructed
// afterwards resumes from it.
func writeBoundaries(t *testing.T, store *dal.Store, ledgerName string, b *raftcmdpb.LedgerBoundaries) {
	t.Helper()

	attrs := attributes.New()
	session := store.OpenWriteSession()
	_, err := attrs.Boundary.Set(session, []byte(ledgerName), b)
	require.NoError(t, err)
	require.NoError(t, session.Commit())
}

// newWorkerForTest builds a Worker wired to a mock source. The proposer is
// nil: every test here returns zero logs from the source, so processBatch
// returns before reaching the propose path.
func newWorkerForTest(t *testing.T, ledgerName string, source v2.Source, store *dal.Store, builder *plan.Builder) *Worker {
	t.Helper()

	ctx := logging.TestingContext()

	return NewWorker(
		ledgerName, 100, source, nil, store, nil, builder,
		logging.FromContext(ctx), noop.NewMeterProvider(),
	)
}
