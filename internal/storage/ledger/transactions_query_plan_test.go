//go:build it

package ledger_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

// TestListTransactionsQueryPlan seeds a ledger with 10 000 transactions
// (mimicking Deriv's metadata pattern) and compares EXPLAIN ANALYZE output
// between the current row_number() window-function query and the proposed
// plain ORDER BY fix. Run with -v to see the full plans.
//
// Usage:
//
//	go test -v -tags it -run TestListTransactionsQueryPlan ./internal/storage/ledger/
func TestListTransactionsQueryPlan(t *testing.T) {
	// Not parallel: EXPLAIN ANALYZE timing numbers are wall-clock measurements.
	// Concurrent test activity would add noise and make the comparison less readable.
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	const txCount = 10_000
	const matchRatio = 10 // every Nth transaction has the source_wallet_id
	walletID := "bench-wallet-deadbeef"

	seedDerivLikeTransactions(t, ctx, store, walletID, txCount, matchRatio)

	ledgerName := store.GetLedger().Name

	// All filter values are embedded directly as SQL literals via fmt.Sprintf
	// so the resulting SQL contains no parameter placeholders.
	srcFilter := fmt.Sprintf(`{"source_wallet_id":"%s"}`, walletID)
	dstFilter := fmt.Sprintf(`{"destination_wallet_id":"%s"}`, walletID)
	currencyFilter := `{"transaction_currency":"USD"}`
	excludeFilter := `{"wallet_transaction_method":"hold_settlement"}`

	// Current implementation: row_number() window function forces full evaluation
	// of all matching rows before LIMIT is applied.
	currentSQL := fmt.Sprintf(`
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH dataset AS (
    SELECT *, row_number() OVER (ORDER BY id DESC)
    FROM (SELECT * FROM "_default".transactions WHERE ledger = '%s') dataset
    WHERE (metadata @> '%s'::jsonb OR metadata @> '%s'::jsonb)
        AND metadata @> '%s'::jsonb
        AND NOT (metadata @> '%s'::jsonb)
    LIMIT 16
)
SELECT * FROM dataset ORDER BY row_number`,
		ledgerName, srcFilter, dstFilter, currencyFilter, excludeFilter)

	// Proposed fix: plain ORDER BY lets the planner use the id DESC index and
	// stop scanning as soon as 16 matching rows are found.
	proposedSQL := fmt.Sprintf(`
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH dataset AS (
    SELECT *
    FROM (SELECT * FROM "_default".transactions WHERE ledger = '%s') dataset
    WHERE (metadata @> '%s'::jsonb OR metadata @> '%s'::jsonb)
        AND metadata @> '%s'::jsonb
        AND NOT (metadata @> '%s'::jsonb)
    ORDER BY id DESC
    LIMIT 16
)
SELECT * FROM dataset ORDER BY id DESC`,
		ledgerName, srcFilter, dstFilter, currencyFilter, excludeFilter)

	t.Log("\n=== Current query plan (row_number window function) ===")
	currentPlan := explainAnalyze(t, ctx, store, currentSQL)
	t.Log(currentPlan)

	t.Log("\n=== Proposed query plan (plain ORDER BY) ===")
	proposedPlan := explainAnalyze(t, ctx, store, proposedSQL)
	t.Log(proposedPlan)

	currentTime := extractPlanningExecutionTime(currentPlan)
	proposedTime := extractPlanningExecutionTime(proposedPlan)
	t.Logf("\nCurrent total time:  %s", currentTime)
	t.Logf("Proposed total time: %s", proposedTime)
}

// seedDerivLikeTransactions inserts txCount transactions directly via SQL.
// Every matchRatio-th row gets source_wallet_id metadata matching walletID.
// No hold_settlement rows are inserted (so the NOT filter always forces a full scan).
func seedDerivLikeTransactions(t *testing.T, ctx context.Context, store *ledgerstore.Store, walletID string, txCount, matchRatio int) {
	t.Helper()

	db := store.GetDB()

	// Embed literal values (all test-controlled). Use %% to produce a literal %
	// for the SQL modulo operator.
	// Non-null sources_arrays suppresses the per-ledger address-segment trigger
	// (trigger condition: new.sources_arrays IS NULL).
	seedSQL := fmt.Sprintf(`
		INSERT INTO "_default".transactions
			(ledger, id, timestamp, postings, sources, destinations,
			 sources_arrays, destinations_arrays, metadata, inserted_at)
		SELECT
			'%s',
			g,
			NOW() - (g || ' seconds')::interval,
			('[{"source":"world","destination":"user:' || g || '","asset":"USD","amount":100}]'),
			'["world"]'::jsonb,
			('["user:' || g || '"]')::jsonb,
			'[["world"]]'::jsonb,
			'[["user"]]'::jsonb,
			CASE WHEN g %% %d = 0
				THEN ('{"source_wallet_id":"%s","transaction_currency":"USD"}')::jsonb
				ELSE '{"transaction_currency":"USD"}'::jsonb
			END,
			NOW()
		FROM generate_series(1, %d) AS g
	`, store.GetLedger().Name, matchRatio, walletID, txCount)

	_, err := db.ExecContext(ctx, seedSQL)
	require.NoError(t, err)

	// Refresh planner statistics so EXPLAIN ANALYZE uses accurate row estimates.
	_, err = db.ExecContext(ctx, `ANALYZE "_default".transactions`)
	require.NoError(t, err)
}

// explainAnalyze runs a no-parameter EXPLAIN ANALYZE and returns the plan text.
// The SQL must have all values embedded as literals (no $N or ? placeholders).
func explainAnalyze(t *testing.T, ctx context.Context, store *ledgerstore.Store, explainSQL string) string {
	t.Helper()

	rows, err := store.GetDB().QueryContext(ctx, explainSQL)
	require.NoError(t, err)
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())

	return strings.Join(lines, "\n")
}

// extractPlanningExecutionTime returns the Planning Time and Execution Time lines.
func extractPlanningExecutionTime(plan string) string {
	var parts []string
	for _, line := range strings.Split(plan, "\n") {
		if strings.Contains(line, "Planning Time") || strings.Contains(line, "Execution Time") {
			parts = append(parts, strings.TrimSpace(line))
		}
	}
	return strings.Join(parts, " | ")
}
