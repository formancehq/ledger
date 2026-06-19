package events_test

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

// newPlanBuilder builds a minimal plan.Builder for events external
// tests that need to drive proposeSinkUpdate through Builder.Run.
func newPlanBuilder(t *testing.T, store *dal.Store) *plan.Builder {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	testCache, err := cache.New(100, meter)
	require.NoError(t, err)

	return plan.NewBuilder(node.NewIndexTracker(1), testCache, attributes.New(), store, nil, logger, 0)
}
