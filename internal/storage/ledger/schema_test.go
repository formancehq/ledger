//go:build it

package ledger_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/logging"

	ledger "github.com/formancehq/ledger/internal"
)

func TestSchemaUpdate(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	store := newLedgerStore(t)

	schema, err := ledger.NewSchema("1.0", ledger.SchemaData{
		Chart: map[string]ledger.ChartSegment{},
	})
	require.NoError(t, err)
	err = store.InsertSchema(ctx, &schema)
	require.NoError(t, err)
	require.Equal(t, "1.0", schema.Version)
	require.NotZero(t, schema.CreatedAt)
}
