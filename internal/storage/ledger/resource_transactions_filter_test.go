package ledger

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/ledger/internal/queries"
	"github.com/formancehq/ledger/internal/storage/common"
)

// renderTransactionsFilter builds the transactions dataset for an alone-in-bucket
// store, applies a single filter the same way ResourceRepository does, and
// renders the SQL with the pg dialect (no live connection required).
func renderTransactionsFilter(t *testing.T, operator, property string, value any) string {
	t.Helper()

	store := newTestStore(false)
	store.aloneInBucket = &atomic.Bool{}
	store.aloneInBucket.Store(true)

	handler := transactionsResourceHandler{store: store}
	dataset, err := handler.BuildDataset(common.RepositoryHandlerBuildContext[any]{})
	require.NoError(t, err)

	where, args, err := handler.ResolveFilter(common.ResourceQuery[any]{}, operator, property, value)
	require.NoError(t, err)

	db := store.db.(*bun.DB)
	q := db.NewSelect().TableExpr("(?) dataset", dataset).Where(where, args...)
	sql, err := q.AppendQuery(db.QueryGen(), nil)
	require.NoError(t, err)
	return string(sql)
}

// renderTransactionsQuery builds the transactions query through query.Builder
// the same way ResourceRepository.buildFilteredDataset does in production.
func renderTransactionsQuery(t *testing.T, aloneInBucket bool, builder query.Builder) string {
	t.Helper()

	store := newTestStore(false)
	store.aloneInBucket = &atomic.Bool{}
	store.aloneInBucket.Store(aloneInBucket)

	handler := transactionsResourceHandler{store: store}
	dataset, err := handler.BuildDataset(common.RepositoryHandlerBuildContext[any]{})
	require.NoError(t, err)

	db := store.db.(*bun.DB)
	q := db.NewSelect().TableExpr("(?) dataset", dataset)

	if builder != nil {
		where, args, err := builder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			return handler.ResolveFilter(common.ResourceQuery[any]{}, operator, key, value)
		}))
		require.NoError(t, err)
		if where != "" {
			q = q.Where(where, args...)
		}
	}

	sql, err := q.AppendQuery(db.QueryGen(), nil)
	require.NoError(t, err)
	return string(sql)
}

func TestTransactionsReferenceFilterEmitsLedgerPredicate(t *testing.T) {
	t.Parallel()

	t.Run("$in string slice", func(t *testing.T) {
		t.Parallel()
		sql := renderTransactionsFilter(t, queries.OperatorIn, "reference", []string{"ref1", "ref2"})
		require.Contains(t, sql, `(ledger = 'ledger0' AND reference IN ('ref1', 'ref2'))`)
	})

	t.Run("$in any slice (validated API input shape)", func(t *testing.T) {
		t.Parallel()
		sql := renderTransactionsFilter(t, queries.OperatorIn, "reference", []any{"ref1", "ref2"})
		require.Contains(t, sql, `(ledger = 'ledger0' AND reference IN ('ref1', 'ref2'))`)
	})

	t.Run("$match", func(t *testing.T) {
		t.Parallel()
		sql := renderTransactionsFilter(t, queries.OperatorMatch, "reference", "ref1")
		require.Contains(t, sql, `(ledger = 'ledger0' AND reference = 'ref1')`)
	})
}

func TestTransactionsReferenceFilterLogicalBuilderComposition(t *testing.T) {
	t.Parallel()

	t.Run("nested $and with account and reference $in", func(t *testing.T) {
		t.Parallel()
		builder := query.And(
			query.Match("account", "users:1"),
			query.In("reference", []any{"ref1", "ref2"}),
		)
		sql := renderTransactionsQuery(t, true, builder)
		require.Contains(t, sql, `(ledger = 'ledger0' AND reference IN ('ref1', 'ref2'))`)
	})

	t.Run("mixed $or between references", func(t *testing.T) {
		t.Parallel()
		builder := query.Or(
			query.Match("reference", "ref1"),
			query.Match("reference", "ref2"),
		)
		sql := renderTransactionsQuery(t, true, builder)
		require.Contains(t, sql, `(((ledger = 'ledger0' AND reference = 'ref1')) or ((ledger = 'ledger0' AND reference = 'ref2')))`)
	})

	t.Run("nested $not", func(t *testing.T) {
		t.Parallel()
		builder := query.Not(query.Match("reference", "ref1"))
		sql := renderTransactionsQuery(t, true, builder)
		require.Contains(t, sql, `not ((ledger = 'ledger0' AND reference = 'ref1'))`)
	})

	t.Run("shared bucket includes ledger scoping in inner dataset", func(t *testing.T) {
		t.Parallel()
		builder := query.Match("reference", "ref1")
		sql := renderTransactionsQuery(t, false, builder)
		// Inner dataset has ledger = 'ledger0', and outer filter also has ledger = 'ledger0'
		require.Contains(t, sql, `WHERE (ledger = 'ledger0')`)
		require.Contains(t, sql, `(ledger = 'ledger0' AND reference = 'ref1')`)
	})
}

func TestTransactionsIDFilterKeepsAloneInBucketOptimization(t *testing.T) {
	t.Parallel()

	// id is served by transactions_id_desc / transactions_sequences, so the
	// alone-in-bucket optimization must still omit the ledger predicate.
	sql := renderTransactionsFilter(t, queries.OperatorMatch, "id", 1)
	require.NotContains(t, sql, `ledger =`)
}
