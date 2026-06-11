package ledger

import (
	"sync/atomic"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// renderScopedSelect builds the scoped select for a freshly created store and
// renders it to SQL using the pg dialect (no live connection required).
func renderScopedSelect(t *testing.T, store *Store) string {
	t.Helper()

	q := store.newScopedSelect().Table("transactions")
	sql, err := q.AppendQuery(store.db.(*bun.DB).Formatter(), nil)
	require.NoError(t, err)
	return string(sql)
}

func newTestStore(disableOptimization bool) *Store {
	db := bun.NewDB(nil, pgdialect.New())
	return &Store{
		db:                              db,
		ledger:                          ledger.Ledger{Name: "ledger0"},
		disableScopedSelectOptimization: disableOptimization,
	}
}

func TestNewScopedSelect_AloneInBucketSkipsPredicate(t *testing.T) {
	t.Parallel()

	store := newTestStore(false)
	store.aloneInBucket = &atomic.Bool{}
	store.aloneInBucket.Store(true)

	require.NotContains(t, renderScopedSelect(t, store), `ledger =`,
		"alone-in-bucket optimization should skip the ledger predicate")
}

func TestNewScopedSelect_NotAloneEmitsPredicate(t *testing.T) {
	t.Parallel()

	store := newTestStore(false)
	store.aloneInBucket = &atomic.Bool{}
	store.aloneInBucket.Store(false)

	require.Contains(t, renderScopedSelect(t, store), `ledger = 'ledger0'`,
		"a ledger sharing its bucket must be filtered by name")
}

func TestNewScopedSelect_DisableOptimizationAlwaysEmitsPredicate(t *testing.T) {
	t.Parallel()

	// Even when the store is flagged as alone in its bucket, disabling the
	// optimization must force the predicate to be emitted.
	store := newTestStore(true)
	store.aloneInBucket = &atomic.Bool{}
	store.aloneInBucket.Store(true)

	require.Contains(t, renderScopedSelect(t, store), `ledger = 'ledger0'`,
		"disabling the optimization must always emit the ledger predicate")
}

func TestWithDisableScopedSelectOptimization(t *testing.T) {
	t.Parallel()

	store := &Store{}
	WithDisableScopedSelectOptimization(true)(store)
	require.True(t, store.disableScopedSelectOptimization)

	WithDisableScopedSelectOptimization(false)(store)
	require.False(t, store.disableScopedSelectOptimization)
}
