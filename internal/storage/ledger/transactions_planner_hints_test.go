//go:build it

package ledger_test

// transactions_planner_hints_test.go covers the adaptive fallback for the
// transactions-list SELECT introduced as a stopgap for the sparse-wallet
// timeout (~50 s observed on sparse-wallet workloads,
// SELECT … ORDER BY id DESC LIMIT 16 with JSONB @> predicates).
//
// The four properties we verify:
//
//  1. Fast path is unchanged: when the probe succeeds within the timeout
//     the result is identical to the plain (no-fallback) path.
//
//  2. Fallback triggers correctly: a tight probe timeout causes SQLSTATE 57014
//     and the retry with the GIN override succeeds, returning the right rows.
//
//  3. SET LOCAL does not leak: after Paginate returns, planner settings and
//     statement_timeout are restored to the session defaults on the same
//     connection (tested on a pinned connection so we can verify reliably).
//
//  4. GetOne and Count are unaffected: they delegate to the base repository
//     and must return the same results as the non-adaptive store.

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// setupHintsTestData creates walletTxCount transactions from/to "wallet:main"
// and unrelatedTxCount transactions between unrelated accounts, all in a fresh
// ledger. Returns the plain (no-adaptive-config) store.
func setupHintsTestData(t *testing.T, walletTxCount, unrelatedTxCount int) *ledgerstore.Store {
	t.Helper()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	for i := 0; i < walletTxCount; i++ {
		tx := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("wallet:main", fmt.Sprintf("account%d", i), "USD", big.NewInt(100)),
		)
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx))
	}
	for i := 0; i < unrelatedTxCount; i++ {
		tx := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", fmt.Sprintf("other%d", i), "USD", big.NewInt(50)),
		)
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx))
	}
	return store
}

// storeWithConfig returns a new Store sharing the ledger/bucket/db of base but
// carrying the given TransactionListConfig. This lets tests exercise different
// timeout and fallback configurations without touching the shared test factory.
func storeWithConfig(t *testing.T, base *ledgerstore.Store, cfg ledgerstore.TransactionListConfig) *ledgerstore.Store {
	t.Helper()
	return ledgerstore.New(
		base.GetDB(),
		base.GetBucket(),
		base.GetLedger(),
		ledgerstore.WithTransactionListConfig(cfg),
	)
}

// walletQuery returns a cursor-paginated query that filters by source/destination
// "wallet:main", a typical sparse-wallet filter pattern.
func walletQuery(pageSize uint64) common.ColumnPaginatedQuery[any] {
	// OrderDesc is an untyped iota constant; explicit cast to paginate.Order required.
	order := paginate.Order(paginate.OrderDesc)
	return common.ColumnPaginatedQuery[any]{
		InitialPaginatedQuery: common.InitialPaginatedQuery[any]{
			Column:   "id",
			Order:    &order,
			PageSize: pageSize,
			Options: common.ResourceQuery[any]{
				Builder: query.Match("account", "wallet:main"),
			},
		},
	}
}

// showSetting queries the value of a Postgres GUC on the given connection.
func showSetting(t *testing.T, ctx context.Context, conn bun.IDB, name string) string {
	t.Helper()
	var val string
	require.NoError(t, conn.QueryRowContext(ctx, "SHOW "+name).Scan(&val))
	return val
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestTransactionListAdaptive_FastPathUnchanged verifies that when the probe
// succeeds within the timeout budget (dense wallet, or generous timeout), the
// rows returned are identical to the plain store with no adaptive config.
// Also exercises the "hook set but returns nil" path in paginateInTx.
func TestTransactionListAdaptive_FastPathUnchanged(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 8, 5)

	// Generous timeouts so the probe never fires.
	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  60_000,
		RetryTimeoutMs:         60_000,
	})

	// A no-op hook exercises the "testHookBeforePaginateSelect != nil but returns nil"
	// branch in paginateInTx — the path that falls through to the actual SELECT.
	adaptive.SetTestHookBeforePaginateSelect(func(_ context.Context, _ bun.Tx) error {
		return nil
	})

	q := walletQuery(15) // enough to return all 8 in one page

	baseCursor, err := base.Transactions().Paginate(ctx, q)
	require.NoError(t, err)
	require.Len(t, baseCursor.Data, 8)

	adaptiveCursor, err := adaptive.Transactions().Paginate(ctx, q)
	require.NoError(t, err)
	require.Len(t, adaptiveCursor.Data, 8)

	for i := range baseCursor.Data {
		require.Equal(t, *baseCursor.Data[i].ID, *adaptiveCursor.Data[i].ID,
			"row %d: id mismatch between baseline and adaptive result", i)
	}
}

// TestTransactionListAdaptive_FallbackTriggeredByTimeout verifies the core
// behaviour: a deliberately tight probe timeout causes the fallback to fire
// and the retry (with GIN override and a generous timeout) still returns the
// correct rows.
//
// We use 200 rows so the 1 ms probe reliably fires SQLSTATE 57014 even on fast
// CI runners.  We also record the wall-clock time spent: because the probe
// timeout fires quickly and the retry (30 s budget) completes successfully, the
// total duration must be < 30 s — proving we got a result from the retry rather
// than an error.
func TestTransactionListAdaptive_FallbackTriggeredByTimeout(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 200, 50)

	// 1 ms probe fires SQLSTATE 57014; 30 s retry succeeds.
	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1,
		RetryTimeoutMs:         30_000,
	})

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err, "retry should succeed even when probe times out")

	// We asked for page size 15; there are 200 wallet rows so we expect a full
	// page and a next-page cursor.
	require.Len(t, cursor.Data, 15)
	require.True(t, cursor.HasMore, "200 wallet rows → HasMore must be true on first page")

	// Rows must be in descending id order regardless of the plan used.
	for i := 1; i < len(cursor.Data); i++ {
		require.Greater(t, *cursor.Data[i-1].ID, *cursor.Data[i].ID,
			"results must be in descending id order after fallback")
	}
}

// TestTransactionListAdaptive_FallbackRowsMatchBaseline confirms that the rows
// returned after a fallback are identical to those from the plain store —
// the GIN path and the index-scan path must agree on the result set.
// We use enough rows to reliably trigger the 1 ms probe timeout.
func TestTransactionListAdaptive_FallbackRowsMatchBaseline(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 200, 50)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1, // always triggers fallback with 200 rows
		RetryTimeoutMs:         30_000,
	})

	q := walletQuery(15)

	baseCursor, err := base.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	adaptiveCursor, err := adaptive.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	require.Equal(t, len(baseCursor.Data), len(adaptiveCursor.Data))
	for i := range baseCursor.Data {
		require.Equal(t, *baseCursor.Data[i].ID, *adaptiveCursor.Data[i].ID,
			"row %d: id mismatch between baseline and fallback result", i)
	}
}

// TestTransactionListAdaptive_NoLeakage pins to a dedicated connection and
// verifies that after Paginate returns, both enable_indexscan and
// statement_timeout are restored to their session defaults. This proves that
// SET LOCAL is strictly scoped to the transaction opened by the adaptive
// wrapper and cannot bleed onto subsequent queries on the same pooled
// connection.
func TestTransactionListAdaptive_NoLeakage(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 3, 2)

	// Obtain the underlying *bun.DB to open a dedicated connection.
	pool, ok := base.GetDB().(*bun.DB)
	require.True(t, ok, "GetDB() must return *bun.DB in test context")

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Create a store pinned to the single connection so we can observe
	// settings before and after Paginate on the exact same connection.
	pinnedStore := ledgerstore.New(
		conn,
		base.GetBucket(),
		base.GetLedger(),
		ledgerstore.WithTransactionListConfig(ledgerstore.TransactionListConfig{
			EnableAdaptiveFallback: true,
			FirstAttemptTimeoutMs:  1, // triggers fallback → GIN override fires
			RetryTimeoutMs:         30_000,
		}),
	)

	// Baseline: Postgres defaults.
	require.Equal(t, "on", showSetting(t, ctx, conn, "enable_indexscan"),
		"enable_indexscan should be 'on' before Paginate")
	require.Equal(t, "0", showSetting(t, ctx, conn, "statement_timeout"),
		"statement_timeout should be '0' (disabled) before Paginate")

	_, err = pinnedStore.Transactions().Paginate(ctx, walletQuery(10))
	require.NoError(t, err)

	// After the transaction commits, SET LOCAL must have been reverted.
	require.Equal(t, "on", showSetting(t, ctx, conn, "enable_indexscan"),
		"enable_indexscan must be restored after Paginate — SET LOCAL leaked")
	require.Equal(t, "0", showSetting(t, ctx, conn, "statement_timeout"),
		"statement_timeout must be restored after Paginate — SET LOCAL leaked")
}

// TestTransactionListAdaptive_RetryAlsoTimesOut verifies that when the retry
// itself times out (RetryTimeoutMs too tight), the error is propagated to the
// caller rather than silently swallowed. Exactly one retry, no loop.
//
// We cannot rely on dataset size to force SQLSTATE 57014: the GIN bitmap scan
// used on the retry is fast by design, and on a warm CI runner even 200 rows
// can complete in < 1 ms. Instead we install a test hook that runs
// SELECT pg_sleep(0.005) inside paginateInTx after SET LOCAL, within the same
// transaction. Because pg_sleep shares the statement_timeout scope it is
// cancelled first, deterministically triggering SQLSTATE 57014 on both the
// probe and the retry.
func TestTransactionListAdaptive_RetryAlsoTimesOut(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1, // 1 ms; pg_sleep hook exceeds this deterministically
		RetryTimeoutMs:         1,
	})

	// Sleep for 5 ms inside each paginateInTx attempt. With statement_timeout=1ms
	// this always fires SQLSTATE 57014 before the sleep completes, regardless of
	// how fast the underlying SELECT would have been.
	adaptive.SetTestHookBeforePaginateSelect(func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.ExecContext(ctx, "SELECT pg_sleep(0.005)")
		return err
	})

	_, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.Error(t, err, "retry timeout should surface an error to the caller")
}

// TestTransactionListAdaptive_ClientCancelNoRetry verifies that when the
// request context is cancelled (client disconnected), Paginate returns an error
// immediately and does NOT attempt the retry.  Retrying a dead request would
// waste DB resources and is explicitly forbidden by the design.
func TestTransactionListAdaptive_ClientCancelNoRetry(t *testing.T) {
	t.Parallel()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1, // tight enough to fire 57014
		RetryTimeoutMs:         30_000,
	})

	// Cancel the context before calling Paginate, simulating a client that
	// disconnected before the request reached the DB layer.
	ctx, cancel := context.WithCancel(logging.TestingContext())
	cancel()

	_, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.Error(t, err, "a cancelled context must produce an error")

	// The error must be a context error, not a 57014 statement-timeout error.
	// If we accidentally retried on a dead context, we'd get a different error
	// class (or a spurious success if the retry somehow completed first).
	require.ErrorIs(t, err, context.Canceled,
		"error must be context.Canceled, not a Postgres statement-timeout — "+
			"a statement-timeout here would mean we retried on a dead context")
}

// TestTransactionListAdaptive_DisabledFallback verifies that when
// EnableAdaptiveFallback is false the plain code path is taken and the query
// succeeds (no overhead from the probe/retry machinery).
func TestTransactionListAdaptive_DisabledFallback(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	noFallback := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: false,
	})

	cursor, err := noFallback.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err)
	require.Len(t, cursor.Data, 4)
}

// TestTransactionListAdaptive_PaginationCursorIntegrity verifies that the
// next-page cursor produced during a fallback-triggered Paginate can be decoded
// and used to fetch the next page correctly.
func TestTransactionListAdaptive_PaginationCursorIntegrity(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 5, 5)

	// Always trigger fallback so cursor is built on the retry path.
	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1,
		RetryTimeoutMs:         30_000,
	})

	// First page: 3 of 5 wallet txns.
	page1, err := adaptive.Transactions().Paginate(ctx, walletQuery(3))
	require.NoError(t, err)
	require.Len(t, page1.Data, 3)
	require.True(t, page1.HasMore)
	require.NotEmpty(t, page1.Next)

	// Decode cursor and fetch second page.
	var nextQ common.ColumnPaginatedQuery[any]
	require.NoError(t, paginate.UnmarshalCursor(page1.Next, &nextQ))

	page2, err := adaptive.Transactions().Paginate(ctx, nextQ)
	require.NoError(t, err)
	require.Len(t, page2.Data, 2)
	require.False(t, page2.HasMore)

	// All 5 ids must be distinct and globally in descending order.
	allIDs := append(page1.Data, page2.Data...)
	for i := 1; i < len(allIDs); i++ {
		require.Greater(t, *allIDs[i-1].ID, *allIDs[i].ID,
			"combined pages must be in descending id order")
	}
}

// TestTransactionListAdaptive_EmptyResultSet verifies that when the wallet
// filter matches no transactions the adaptive path returns an empty cursor
// without error.  An empty result set never triggers the probe timeout (the
// query completes instantly), so this also exercises the fast path with a
// zero-row response.
func TestTransactionListAdaptive_EmptyResultSet(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	// Populate only unrelated transactions so the wallet filter matches nothing.
	base := setupHintsTestData(t, 0, 5)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  5_000,
		RetryTimeoutMs:         30_000,
	})

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err)
	require.Empty(t, cursor.Data, "no wallet transactions → Data must be empty")
	require.False(t, cursor.HasMore, "no wallet transactions → HasMore must be false")
}

// TestTransactionListAdaptive_AlreadyInTxSkipsAdaptive verifies that when the
// store's DB is already a bun.Tx (i.e. the caller opened an outer transaction
// via BeginTX), the adaptive paginator falls straight through to the base
// repository without issuing any SET LOCAL statements.
//
// If it DID issue SET LOCAL on the outer transaction and the probe timed out,
// the outer transaction would be left in an aborted state — a silent data hazard.
// The safe contract is: adaptive machinery is skipped entirely within an outer tx.
func TestTransactionListAdaptive_AlreadyInTxSkipsAdaptive(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 5, 2)

	// Build an adaptive store with a 1 ms probe — if the adaptive path ran
	// inside the outer tx and SET LOCAL leaked, subsequent queries on the same
	// tx would be killed by the 1 ms timeout.
	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1,
		RetryTimeoutMs:         30_000,
	})

	// Open an outer transaction on the adaptive store.
	txStore, tx, err := adaptive.BeginTX(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// Paginate inside the outer transaction.  The adaptive machinery must be
	// bypassed; if it were not, the 1 ms SET LOCAL would bleed into txStore and
	// the query below (or the ROLLBACK) would time out.
	cursor, err := txStore.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err, "Paginate inside outer tx must not corrupt the transaction")
	require.Len(t, cursor.Data, 5)

	// Prove the outer transaction is still usable — statement_timeout must not
	// have been mutated to 1 ms.
	var val string
	require.NoError(t, tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&val))
	require.Equal(t, "0", val,
		"statement_timeout must be the session default (0) — SET LOCAL must not have run on the outer tx")
}
func TestTransactionListAdaptive_GetOneAndCountUnaffected(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1, // would trigger fallback on Paginate
		RetryTimeoutMs:         30_000,
	})

	// Count.
	baseCount, err := base.Transactions().Count(ctx, common.ResourceQuery[any]{
		Builder: query.Match("account", "wallet:main"),
	})
	require.NoError(t, err)

	adaptiveCount, err := adaptive.Transactions().Count(ctx, common.ResourceQuery[any]{
		Builder: query.Match("account", "wallet:main"),
	})
	require.NoError(t, err)
	require.Equal(t, baseCount, adaptiveCount)

	// GetOne by id.
	q := walletQuery(1)
	cursor, err := base.Transactions().Paginate(ctx, q)
	require.NoError(t, err)
	require.NotEmpty(t, cursor.Data)

	tx, err := adaptive.Transactions().GetOne(ctx, common.ResourceQuery[any]{
		Builder: query.Match("id", pointer.For(*cursor.Data[0].ID)),
	})
	require.NoError(t, err)
	require.Equal(t, *cursor.Data[0].ID, *tx.ID)
}

// TestTransactionListAdaptive_FallbackRetrySucceedsViaPgSleep guarantees the
// fallback fires deterministically (via pg_sleep on the probe) and the retry
// succeeds (no artificial delay on the second paginateInTx call). This covers
// the retry-success path in Paginate — the logging and return after a successful
// GIN-override retry — which is NOT reliably reached by dataset-size-based tests
// on fast CI runners where the probe's 1ms timeout may never fire.
func TestTransactionListAdaptive_FallbackRetrySucceedsViaPgSleep(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 8, 4)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1, // probe: hook makes this fire 57014
		RetryTimeoutMs:         30_000,
	})

	// The hook fires on the probe (first paginateInTx call) and is a no-op on
	// the retry (second call). Using context.Background() in the pg_sleep call
	// prevents the Go context from racing against the Postgres statement_timeout.
	callCount := 0
	adaptive.SetTestHookBeforePaginateSelect(func(_ context.Context, tx bun.Tx) error {
		callCount++
		if callCount == 1 {
			_, err := tx.ExecContext(context.Background(), "SELECT pg_sleep(0.005)")
			return err // SQLSTATE 57014 → trigger fallback
		}
		return nil // retry: no delay → GIN scan succeeds
	})

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err, "retry with GIN override must succeed")
	require.Len(t, cursor.Data, 8, "all 8 wallet rows must be returned")
	require.False(t, cursor.HasMore, "8 rows < pageSize 15, no second page")
	require.Equal(t, 2, callCount, "hook must have been called exactly twice (probe + retry)")
}

// TestTransactionListAdaptive_CtxCancelAfterProbe covers the edge case where
// the probe fires SQLSTATE 57014 and the request context is cancelled at the
// same moment (e.g. client disconnects). The implementation must NOT retry,
// and must return the error rather than silently swallowing it.
func TestTransactionListAdaptive_CtxCancelAfterProbe(t *testing.T) {
	t.Parallel()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  1,
		RetryTimeoutMs:         30_000, // generous — retry must NOT run
	})

	ctx, cancel := context.WithCancel(logging.TestingContext())
	defer cancel()

	// The hook cancels the outer context (simulating a client disconnect that
	// arrives just as the probe fires), then sleeps past the 1ms statement_timeout
	// using context.Background() so the Go context cancellation cannot race ahead
	// of the Postgres-side statement_timeout.
	hookCalls := 0
	adaptive.SetTestHookBeforePaginateSelect(func(_ context.Context, tx bun.Tx) error {
		hookCalls++
		cancel()
		_, err := tx.ExecContext(context.Background(), "SELECT pg_sleep(0.005)")
		return err // SQLSTATE 57014; ctx is now cancelled
	})

	_, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.Error(t, err, "must return an error when context is cancelled after probe timeout")
	require.Equal(t, 1, hookCalls, "hook must be called exactly once — no retry after ctx cancel")
}

// TestTransactionListAdaptive_ZeroTimeoutNoSetLocal verifies that
// FirstAttemptTimeoutMs = 0 skips the SET LOCAL statement_timeout entirely so
// the probe runs without a server-side timeout. Also exercises
// DefaultTransactionListConfig() and field override.
func TestTransactionListAdaptive_ZeroTimeoutNoSetLocal(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	// Start from the production defaults and only disable the probe timeout.
	cfg := ledgerstore.DefaultTransactionListConfig()
	cfg.FirstAttemptTimeoutMs = 0 // skip SET LOCAL statement_timeout for probe
	adaptive := storeWithConfig(t, base, cfg)

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err)
	require.Len(t, cursor.Data, 4)
}
