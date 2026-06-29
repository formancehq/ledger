//go:build it

package ledger_test

// transactions_planner_hints_test.go covers the hedged-request strategy for the
// transactions-list SELECT introduced as a stopgap for the sparse-wallet
// timeout (~50 s observed on sparse-wallet workloads,
// SELECT … ORDER BY id DESC LIMIT 16 with JSONB @> predicates).
//
// The properties we verify:
//
//  1. Fast path is unchanged: when the original finishes before the chaser
//     delay the result is identical to the plain (no-fallback) path.
//
//  2. Chaser fires and wins: when the original is slow the chaser with GIN
//     override returns the correct rows.
//
//  3. SET LOCAL does not leak: after Paginate returns, planner settings and
//     statement_timeout are restored to the session defaults on the same
//     connection (tested on a pinned connection so we can verify reliably).
//
//  4. GetOne and Count are unaffected: they delegate to the base repository
//     and must return the same results as the non-adaptive store.
//
//  5. Already-in-tx: when the store is already inside a bun.Tx, the hedging
//     machinery is bypassed entirely.

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
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

// TestTransactionListAdaptive_FastPathUnchanged verifies that when the original
// finishes before the chaser delay (dense wallet or generous delay), the rows
// returned are identical to the plain store with no adaptive config.
func TestTransactionListAdaptive_FastPathUnchanged(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 8, 5)

	// Generous chaser delay so the original always wins.
	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          60_000,
		ChaserTimeoutMs:        60_000,
	})

	// A no-op hook exercises the "testHookBeforePaginateSelect != nil but returns nil"
	// branch in paginateInTx.
	adaptive.SetTestHookBeforePaginateSelect(func(_ context.Context, _ bun.Tx, _ bool) error {
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

// TestTransactionListAdaptive_ChaserWins verifies the core hedged-request
// behaviour: when the original is slow, the chaser fires and its GIN-override
// result is returned. The hook blocks the original (pg_sleep) and is a no-op
// for the chaser; the chaser wins the race.
func TestTransactionListAdaptive_ChaserWins(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 200, 50)

	// Chaser fires after 1ms; generous timeout.
	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          1,
		ChaserTimeoutMs:        30_000,
	})

	adaptive.SetTestHookBeforePaginateSelect(func(ctx context.Context, tx bun.Tx, isChaser bool) error {
		if !isChaser {
			// Original: sleep long enough for chaser to fire and win.
			// Use ctx so the sleep is cancelled when the chaser wins.
			_, err := tx.ExecContext(ctx, "SELECT pg_sleep(10)")
			return err
		}
		return nil // Chaser: no delay
	})

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err, "chaser should succeed and win the race")

	// We asked for page size 15; there are 200 wallet rows so we expect a full
	// page and a next-page cursor.
	require.Len(t, cursor.Data, 15)
	require.True(t, cursor.HasMore, "200 wallet rows → HasMore must be true on first page")

	// Rows must be in descending id order regardless of the plan used.
	for i := 1; i < len(cursor.Data); i++ {
		require.Greater(t, *cursor.Data[i-1].ID, *cursor.Data[i].ID,
			"results must be in descending id order after chaser wins")
	}
}

// TestTransactionListAdaptive_ChaserRowsMatchBaseline confirms that the rows
// returned when the chaser wins are identical to those from the plain store —
// the GIN path and the index-scan path must agree on the result set.
func TestTransactionListAdaptive_ChaserRowsMatchBaseline(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 200, 50)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          1,
		ChaserTimeoutMs:        30_000,
	})

	adaptive.SetTestHookBeforePaginateSelect(func(ctx context.Context, tx bun.Tx, isChaser bool) error {
		if !isChaser {
			_, err := tx.ExecContext(ctx, "SELECT pg_sleep(10)")
			return err
		}
		return nil
	})

	q := walletQuery(15)

	baseCursor, err := base.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	adaptiveCursor, err := adaptive.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	require.Equal(t, len(baseCursor.Data), len(adaptiveCursor.Data))
	for i := range baseCursor.Data {
		require.Equal(t, *baseCursor.Data[i].ID, *adaptiveCursor.Data[i].ID,
			"row %d: id mismatch between baseline and chaser result", i)
	}
}

// TestTransactionListAdaptive_NoLeakage pins to a dedicated connection and
// verifies that after Paginate returns, both enable_indexscan and
// statement_timeout are restored to their session defaults. This proves that
// SET LOCAL is strictly scoped to the transactions opened by the hedged
// queries and cannot bleed onto subsequent queries on the same pooled
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
			ChaserDelayMs:          1, // fires chaser
			ChaserTimeoutMs:        30_000,
		}),
	)

	// Force chaser to fire and issue SET LOCAL enable_indexscan = off
	// by making the original slow.
	pinnedStore.SetTestHookBeforePaginateSelect(func(ctx context.Context, tx bun.Tx, isChaser bool) error {
		if !isChaser {
			_, err := tx.ExecContext(ctx, "SELECT pg_sleep(10)")
			return err
		}
		return nil
	})

	// Baseline: Postgres defaults.
	require.Equal(t, "on", showSetting(t, ctx, conn, "enable_indexscan"),
		"enable_indexscan should be 'on' before Paginate")
	require.Equal(t, "0", showSetting(t, ctx, conn, "statement_timeout"),
		"statement_timeout should be '0' (disabled) before Paginate")

	_, err = pinnedStore.Transactions().Paginate(ctx, walletQuery(10))
	require.NoError(t, err)

	// After the transactions commit, SET LOCAL must have been reverted.
	require.Equal(t, "on", showSetting(t, ctx, conn, "enable_indexscan"),
		"enable_indexscan must be restored after Paginate — SET LOCAL leaked")
	require.Equal(t, "0", showSetting(t, ctx, conn, "statement_timeout"),
		"statement_timeout must be restored after Paginate — SET LOCAL leaked")
}

// TestTransactionListAdaptive_BothFail verifies that when both the original and
// the chaser fail, the error is propagated to the caller. The hook makes both
// queries time out via pg_sleep against a tight chaser timeout.
func TestTransactionListAdaptive_BothFail(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          1,
		ChaserTimeoutMs:        1, // 1ms chaser timeout
	})

	// Both original and chaser get a long sleep. The chaser has a 1ms timeout
	// so it gets 57014. The original has no timeout but the context will be
	// cancelled once we drain both results (both fail).
	adaptive.SetTestHookBeforePaginateSelect(func(ctx context.Context, tx bun.Tx, _ bool) error {
		_, err := tx.ExecContext(ctx, "SELECT pg_sleep(10)")
		return err
	})

	_, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.Error(t, err, "both queries failing should surface an error to the caller")
}

// TestTransactionListAdaptive_ClientCancelNoHedge verifies that when the
// request context is cancelled (client disconnected), Paginate returns an error
// immediately and both goroutines are cancelled. No wasted DB resources.
func TestTransactionListAdaptive_ClientCancelNoHedge(t *testing.T) {
	t.Parallel()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          1,
		ChaserTimeoutMs:        30_000,
	})

	// Cancel the context before calling Paginate, simulating a client that
	// disconnected before the request reached the DB layer.
	ctx, cancel := context.WithCancel(logging.TestingContext())
	cancel()

	_, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.Error(t, err, "a cancelled context must produce an error")
	require.ErrorIs(t, err, context.Canceled,
		"error must be context.Canceled, not a Postgres error")
}

// TestTransactionListAdaptive_DisabledFallback verifies that when
// EnableAdaptiveFallback is false the plain code path is taken and the query
// succeeds (no overhead from the hedging machinery). A test hook that would
// fail if called proves the adaptive wrapper is never invoked.
func TestTransactionListAdaptive_DisabledFallback(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	noFallback := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: false,
	})

	noFallback.SetTestHookBeforePaginateSelect(func(_ context.Context, _ bun.Tx, _ bool) error {
		t.Fatal("adaptive hook must not be called when EnableAdaptiveFallback is false")
		return nil
	})

	cursor, err := noFallback.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err)
	require.Len(t, cursor.Data, 4)
}

// TestTransactionListAdaptive_PaginationCursorIntegrity verifies that the
// next-page cursor produced during a hedged Paginate (chaser wins) can be
// decoded and used to fetch the next page correctly.
func TestTransactionListAdaptive_PaginationCursorIntegrity(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 5, 5)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          1,
		ChaserTimeoutMs:        30_000,
	})

	// Force chaser to win so cursor is built on the chaser path.
	adaptive.SetTestHookBeforePaginateSelect(func(ctx context.Context, tx bun.Tx, isChaser bool) error {
		if !isChaser {
			_, err := tx.ExecContext(ctx, "SELECT pg_sleep(10)")
			return err
		}
		return nil
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
// filter matches no transactions the hedged path returns an empty cursor
// without error. An empty result set finishes instantly, so the chaser never
// fires.
func TestTransactionListAdaptive_EmptyResultSet(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	// Populate only unrelated transactions so the wallet filter matches nothing.
	base := setupHintsTestData(t, 0, 5)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          5_000,
		ChaserTimeoutMs:        30_000,
	})

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err)
	require.Empty(t, cursor.Data, "no wallet transactions → Data must be empty")
	require.False(t, cursor.HasMore, "no wallet transactions → HasMore must be false")
}

// TestTransactionListAdaptive_AlreadyInTxSkipsAdaptive verifies that when the
// store's DB is already a bun.Tx (i.e. the caller opened an outer transaction
// via BeginTX), the adaptive paginator falls straight through to the base
// repository without launching goroutines or issuing any SET LOCAL statements.
func TestTransactionListAdaptive_AlreadyInTxSkipsAdaptive(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 5, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          1,
		ChaserTimeoutMs:        30_000,
	})

	// Open an outer transaction on the adaptive store.
	txStore, tx, err := adaptive.BeginTX(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// Paginate inside the outer transaction. The hedging machinery must be
	// bypassed; if it were not, SET LOCAL would bleed into txStore.
	cursor, err := txStore.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err, "Paginate inside outer tx must not corrupt the transaction")
	require.Len(t, cursor.Data, 5)

	// Prove the outer transaction is still usable.
	var val string
	require.NoError(t, tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&val))
	require.Equal(t, "0", val,
		"statement_timeout must be the session default (0) — SET LOCAL must not have run on the outer tx")
}

// TestTransactionListAdaptive_GetOneAndCountUnaffected verifies that GetOne and
// Count delegate to the base repository and are not affected by the hedging.
func TestTransactionListAdaptive_GetOneAndCountUnaffected(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          1,
		ChaserTimeoutMs:        30_000,
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

// TestTransactionListAdaptive_OriginalWinsBeforeChaser verifies that when the
// original query finishes quickly (before the chaser delay), the chaser never
// fires and the result is correct. This is the common case for dense wallets.
func TestTransactionListAdaptive_OriginalWinsBeforeChaser(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 8, 4)

	// 60s chaser delay — original will always win on 8 rows.
	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          60_000,
		ChaserTimeoutMs:        30_000,
	})

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err, "original should succeed before chaser fires")
	require.Len(t, cursor.Data, 8, "all 8 wallet rows must be returned")
	require.False(t, cursor.HasMore, "8 rows < pageSize 15, no second page")
}

// TestTransactionListAdaptive_ZeroDelayFiresChaserImmediately verifies that
// ChaserDelayMs = 0 fires the chaser immediately alongside the original.
// Both race from the start; whichever wins is returned.
func TestTransactionListAdaptive_ZeroDelayFiresChaserImmediately(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	cfg := ledgerstore.DefaultTransactionListConfig()
	cfg.ChaserDelayMs = 0
	adaptive := storeWithConfig(t, base, cfg)

	cursor, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.NoError(t, err)
	require.Len(t, cursor.Data, 4)
}

// TestTransactionListAdaptive_ServerSideCancelReturnsError verifies that a
// server-side 57014 (e.g. from pg_cancel_backend) with a live Go context
// surfaces as an error. In the hedged pattern, if the original fails with a
// non-timeout error, the chaser may still succeed.
func TestTransactionListAdaptive_ServerSideCancelReturnsError(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          60_000, // chaser won't fire in time
		ChaserTimeoutMs:        30_000,
	})

	adaptive.SetTestHookBeforePaginateSelect(func(_ context.Context, _ bun.Tx, _ bool) error {
		return &pgconn.PgError{
			Code:    pgerrcode.QueryCanceled,
			Message: "canceling statement due to user request",
		}
	})

	_, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.Error(t, err, "server-side cancel must surface an error")

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, pgerrcode.QueryCanceled, pgErr.Code)
}

// TestTransactionListAdaptive_CtxCancelDuringOriginal covers the edge case
// where the request context is cancelled while the original is running (e.g.
// client disconnects). Both goroutines must be cancelled and an error returned.
func TestTransactionListAdaptive_CtxCancelDuringOriginal(t *testing.T) {
	t.Parallel()

	base := setupHintsTestData(t, 4, 2)

	adaptive := storeWithConfig(t, base, ledgerstore.TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          60_000, // chaser won't fire
		ChaserTimeoutMs:        30_000,
	})

	ctx, cancel := context.WithCancel(logging.TestingContext())

	// The hook cancels the outer context (simulating client disconnect) then
	// sleeps using the passed context so it respects the cancellation.
	adaptive.SetTestHookBeforePaginateSelect(func(ctx context.Context, tx bun.Tx, _ bool) error {
		cancel()
		_, err := tx.ExecContext(ctx, "SELECT pg_sleep(10)")
		return err
	})

	_, err := adaptive.Transactions().Paginate(ctx, walletQuery(15))
	require.Error(t, err, "must return an error when context is cancelled during original")
}
