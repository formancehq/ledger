//go:build it

package ledger_test

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
)

// sqlRecorder is a bun.QueryHook that captures the formatted SQL of executed queries while enabled.
// It is process-wide (bun has no hook removal), so it filters nothing itself — callers isolate their
// queries by the unique per-test ledger name.
type sqlRecorder struct {
	enabled atomic.Bool
	mu      sync.Mutex
	queries []string
}

func (r *sqlRecorder) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (r *sqlRecorder) AfterQuery(_ context.Context, e *bun.QueryEvent) {
	if !r.enabled.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = append(r.queries, e.Query)
}

func (r *sqlRecorder) snapshotLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.queries)
}

func (r *sqlRecorder) since(from int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.queries[from:]...)
}

// TestListTransactionsMaterializedFence asserts that a selective ("needle") filter emits a
// MATERIALIZED dataset CTE, while an unfiltered list keeps the historical non-materialized shape
// (acceptance criterion 2 regression guard + non-fence shape preservation).
func TestListTransactionsMaterializedFence(t *testing.T) {
	// Not parallel: relies on a process-wide query hook with a recording window.
	store := newLedgerStore(t)
	ctx := logging.TestingContext()
	now := time.Now()

	rec := &sqlRecorder{}
	defaultBunDB.GetValue().AddQueryHook(rec)
	t.Cleanup(func() { rec.enabled.Store(false) })

	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
		WithMetadata(metadata.Metadata{"wallet_id": "w1"}).
		WithTimestamp(now)
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx))

	// capture runs fn with recording enabled and returns the dataset queries it produced. This test
	// is intentionally NOT parallel: a non-parallel test runs while all t.Parallel siblings are
	// paused, so the process-wide recorder only sees this test's queries during the window, and
	// matching on the "dataset" CTE is enough to pick out the transactions list query.
	capture := func(fn func()) []string {
		from := rec.snapshotLen()
		rec.enabled.Store(true)
		fn()
		rec.enabled.Store(false)

		var out []string
		for _, q := range rec.since(from) {
			if strings.Contains(q, "dataset") {
				out = append(out, q)
			}
		}
		return out
	}

	containsMaterialized := func(queries []string) bool {
		for _, q := range queries {
			if strings.Contains(q, "AS MATERIALIZED") {
				return true
			}
		}
		return false
	}

	t.Run("selective account filter is fenced", func(t *testing.T) {
		queries := capture(func() {
			_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
				PageSize: 10,
				Options:  common.ResourceQuery[any]{Builder: query.Match("account", "alice")},
			})
			require.NoError(t, err)
		})
		require.NotEmpty(t, queries, "expected to capture the list query")
		require.True(t, containsMaterialized(queries),
			"selective filter should emit a MATERIALIZED CTE, got: %v", queries)
	})

	t.Run("metadata filter is fenced", func(t *testing.T) {
		queries := capture(func() {
			_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
				PageSize: 10,
				Options:  common.ResourceQuery[any]{Builder: query.Match("metadata[wallet_id]", "w1")},
			})
			require.NoError(t, err)
		})
		require.NotEmpty(t, queries)
		require.True(t, containsMaterialized(queries),
			"metadata filter should emit a MATERIALIZED CTE, got: %v", queries)
	})

	t.Run("unfiltered list is not fenced", func(t *testing.T) {
		queries := capture(func() {
			_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
				PageSize: 10,
			})
			require.NoError(t, err)
		})
		require.NotEmpty(t, queries)
		require.False(t, containsMaterialized(queries),
			"unfiltered list must keep the non-materialized shape, got: %v", queries)
	})

	t.Run("range-only (timestamp) filter is not fenced", func(t *testing.T) {
		queries := capture(func() {
			_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
				PageSize: 10,
				Options:  common.ResourceQuery[any]{Builder: query.Lte("timestamp", now.Format(time.RFC3339Nano))},
			})
			require.NoError(t, err)
		})
		require.NotEmpty(t, queries)
		require.False(t, containsMaterialized(queries),
			"range-only filter must not be fenced, got: %v", queries)
	})

	// Regression guard for the nested-CTE shape: when fenced with effectiveVolumes, the page LIMIT
	// must live inside the "dataset" CTE so the expand's "select id from dataset" sees only the page,
	// not the whole materialized filtered set. Otherwise effectiveVolumes aggregates over the entire
	// matched history to return one page.
	t.Run("fenced + effectiveVolumes pages before the expand reads dataset", func(t *testing.T) {
		queries := capture(func() {
			_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
				PageSize: 5,
				Options: common.ResourceQuery[any]{
					Builder: query.Match("account", "alice"),
					Expand:  []string{"effectiveVolumes"},
				},
			})
			require.NoError(t, err)
		})

		var main string
		for _, q := range queries {
			if strings.Contains(q, "AS MATERIALIZED") {
				main = q
				break
			}
		}
		require.NotEmpty(t, main, "expected a fenced list query, got: %v", queries)

		low := strings.ToLower(main)
		expandRef := strings.Index(low, "select id from dataset")
		limitIdx := strings.Index(low, "limit")
		require.GreaterOrEqual(t, expandRef, 0, "effectiveVolumes expand should reference dataset: %s", main)
		require.GreaterOrEqual(t, limitIdx, 0, "fenced query should carry a page LIMIT: %s", main)
		require.Less(t, limitIdx, expandRef,
			"page LIMIT must live inside the dataset CTE (before the expand reads from dataset), "+
				"so effectiveVolumes aggregates over the page, not the whole filtered set: %s", main)
	})
}

// TestListTransactionsFencedPagination walks next/previous pages under a fenced (selective) filter
// with a page size of 1, confirming cursor pagination, HasMore overfetch, and ordering are correct
// when ORDER BY + LIMIT live on the outer select and the keyset predicate stays inside the fence.
func TestListTransactionsFencedPagination(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()
	now := time.Now()

	// Three transactions all touching "alice" (so the account filter is a needle => fenced),
	// at increasing timestamps so the default id/timestamp DESC order is tx3, tx2, tx1.
	var ids []uint64
	for i := 0; i < 3; i++ {
		tx := ledger.NewTransaction().
			WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
			WithTimestamp(now.Add(time.Duration(i) * time.Minute))
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx))
		ids = append(ids, *tx.ID)
	}
	// Descending id order is the default.
	wantDesc := []uint64{ids[2], ids[1], ids[0]}

	filter := common.InitialPaginatedQuery[any]{
		PageSize: 1,
		Options:  common.ResourceQuery[any]{Builder: query.Match("account", "alice")},
	}

	// Forward traversal collects one transaction per page following Next until exhausted.
	var forward []uint64
	var q common.PaginatedQuery[any] = filter
	for {
		cursor, err := store.Transactions().Paginate(ctx, q)
		require.NoError(t, err)
		require.LessOrEqual(t, len(cursor.Data), 1)
		for _, tx := range cursor.Data {
			forward = append(forward, *tx.ID)
		}
		if cursor.Next == "" {
			break
		}
		q, err = common.UnmarshalCursor[any](cursor.Next)
		require.NoError(t, err)
		require.Less(t, len(forward), 10, "pagination did not terminate")
	}
	require.Equal(t, wantDesc, forward, "fenced forward pagination must return all rows in order")

	// From the last page, walk Previous back to the start and confirm we revisit the same rows.
	last, err := store.Transactions().Paginate(ctx, filter)
	require.NoError(t, err)
	require.NotEmpty(t, last.Next)
	// advance to the 2nd page so a Previous link exists
	second, err := common.UnmarshalCursor[any](last.Next)
	require.NoError(t, err)
	secondCursor, err := store.Transactions().Paginate(ctx, second)
	require.NoError(t, err)
	require.Equal(t, []uint64{wantDesc[1]}, idsOf(secondCursor.Data))
	require.NotEmpty(t, secondCursor.Previous)
	prev, err := common.UnmarshalCursor[any](secondCursor.Previous)
	require.NoError(t, err)
	prevCursor, err := store.Transactions().Paginate(ctx, prev)
	require.NoError(t, err)
	require.Equal(t, []uint64{wantDesc[0]}, idsOf(prevCursor.Data))
}

// TestListTransactionsFencedWithEffectiveVolumes guards the expand interaction: under the fence the
// dataset CTE holds the full filtered set (no inner LIMIT), and the effectiveVolumes expand CTE
// references "select id from dataset". The page must still come back correct with its volumes.
func TestListTransactionsFencedWithEffectiveVolumes(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()
	now := time.Now()

	for i := 0; i < 3; i++ {
		tx := ledger.NewTransaction().
			WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
			WithTimestamp(now.Add(time.Duration(i) * time.Minute))
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx))
	}

	cursor, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
		PageSize: 2,
		Options: common.ResourceQuery[any]{
			Builder: query.Match("account", "alice"),
			Expand:  []string{"volumes", "effectiveVolumes"},
		},
	})
	require.NoError(t, err)
	require.Len(t, cursor.Data, 2, "page size honored under fence")
	require.True(t, cursor.HasMore, "third row should be detected via overfetch")
	for _, tx := range cursor.Data {
		require.NotEmpty(t, tx.PostCommitEffectiveVolumes,
			"effective volumes must be populated for each page row under the fence")
		require.Equal(t, tx.PostCommitVolumes, tx.PostCommitEffectiveVolumes)
	}
}

func idsOf(txs []ledger.Transaction) []uint64 {
	out := make([]uint64, 0, len(txs))
	for _, tx := range txs {
		out = append(out, *tx.ID)
	}
	return out
}
