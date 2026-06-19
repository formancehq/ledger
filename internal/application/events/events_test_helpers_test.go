package events

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// newTestBuilder spins up a minimal plan.Builder against a temp Pebble
// store so emitter / manager tests can exercise the proposeSinkUpdate
// path that routes through Builder.Run. The store is closed on test
// cleanup.
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
