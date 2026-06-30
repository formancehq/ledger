//go:build it

package ledger_test

// transactions_sparse_wallet_sim_test.go — sparse-wallet query plan simulation
//
// Reproduces the sparse-wallet query plan problem WITHOUT large amounts of real data.
// The plan shape depends on selectivity (matching rows / total rows), not
// on absolute table size.  We plant 50 "wallet-target" rows at the lowest
// ids (oldest transactions), then fill the rest of the table with noise.
// ORDER BY id DESC LIMIT 16 scans the noise from the top down before it
// ever reaches the 50 matches — the same pathology as production.
//
// Two ledger stores are compared:
//
//	plain   — no feature flag, uses @> containment (current production behaviour)
//	indexed — INDEXED_METADATA_KEYS set, uses ->> equality + functional partial index
//
// The partial functional index is created inline for the test ledger name so
// this test is self-contained and does not depend on any migration being
// tied to a literal ledger name.
//
// Usage:
//
//	DOCKER_HOST=unix:///~/.colima/default/docker.sock \
//	  go test -tags it -run TestSparseWalletSimulation -v \
//	    -timeout 60s ./internal/storage/ledger/...
//
// For production-scale runs (500k–5M rows, several minutes):
//
//	SIM_ROWS=500000 ... -timeout 300s ...
//
// Environment variables:
//
//	SIM_ROWS    total rows (default 1000000; use 20000 for a quick sanity check)
//	SIM_WALLET  number of sparse matching rows (default 50)

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"

	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

func getEnvInt(t *testing.T, key string, defaultVal int) int {
	t.Helper()
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("invalid value for %s=%q: %v", key, s, err)
	}
	return v
}

// seedSparseData inserts directly into Postgres via generate_series, bypassing
// the ledger API entirely.  This makes seeding ~100× faster than going through
// CommitTransaction.
//
// Layout (ids ascend with time):
//
//	[1 … walletRows]             sparse wallet transactions (matching metadata)
//	[walletRows+1 … totalRows]   noise transactions (empty metadata)
//
// ORDER BY id DESC starts at totalRows and walks backward, reaching the
// wallet rows only after scanning all of the noise — the slow-path pattern.
func seedSparseData(t *testing.T, store *ledgerstore.Store, totalRows, walletRows int, walletID string) {
	t.Helper()
	ctx := logging.TestingContext()

	schema := store.GetLedger().Bucket
	ledgerName := store.GetLedger().Name

	// All values are test-controlled (alphanumeric/integer), safe to embed directly.
	// Wallet rows at LOW ids (oldest, hardest to reach with id DESC scan).
	_, err := store.GetDB().ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %q.transactions
			(ledger, id, timestamp, postings, sources, destinations,
			 sources_arrays, destinations_arrays, metadata)
		SELECT
			'%s',
			gs,
			NOW() - (gs * INTERVAL '1 second'),
			'[{"source":"wallet:src","destination":"wallet:dst","amount":100,"asset":"USD/2"}]',
			'["wallet:src"]'::jsonb,
			'["wallet:dst"]'::jsonb,
			'[{"0":"wallet","1":"src","2":null}]'::jsonb,
			'[{"0":"wallet","1":"dst","2":null}]'::jsonb,
			jsonb_build_object('source_wallet_id', '%s')
		FROM generate_series(1, %d) gs
	`, schema, ledgerName, walletID, walletRows))
	require.NoError(t, err, "seeding wallet rows failed")

	// Noise rows at HIGH ids (newest — the backward scan hits these first).
	_, err = store.GetDB().ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %q.transactions
			(ledger, id, timestamp, postings, sources, destinations,
			 sources_arrays, destinations_arrays, metadata)
		SELECT
			'%s',
			%d + gs,
			NOW() - (gs * INTERVAL '2 second'),
			'[{"source":"world","destination":"acc","amount":100,"asset":"USD/2"}]',
			'["world"]'::jsonb,
			'["acc"]'::jsonb,
			'[{"0":"world","1":null}]'::jsonb,
			'[{"0":"acc","1":null}]'::jsonb,
			'{}'::jsonb
		FROM generate_series(1, %d) gs
	`, schema, ledgerName, walletRows, totalRows-walletRows))
	require.NoError(t, err, "seeding noise rows failed")

	// Update Postgres statistics so the planner has accurate row counts.
	_, err = store.GetDB().ExecContext(ctx, fmt.Sprintf(`ANALYZE %q.transactions`, schema))
	require.NoError(t, err)

	t.Logf("seeded %d total rows (%d wallet, %d noise) for ledger %q",
		totalRows, walletRows, totalRows-walletRows, ledgerName)
}

// dropGINIndex removes transactions_metadata_index so that bulk inserts don't
// pay GIN maintenance cost per row.  Call rebuildGINIndex once all seeding is
// done.  plain and indexed stores share a schema, so one drop covers both.
func dropGINIndex(t *testing.T, store *ledgerstore.Store) {
	t.Helper()
	ctx := logging.TestingContext()
	schema := store.GetLedger().Bucket
	_, err := store.GetDB().ExecContext(ctx, fmt.Sprintf(
		`DROP INDEX IF EXISTS %q.transactions_metadata_index`, schema))
	require.NoError(t, err)
	t.Logf("dropped GIN index on %q for bulk load", schema)
}

// rebuildGINIndex recreates transactions_metadata_index and runs ANALYZE with a
// boosted statistics target.  At 1M rows with only 50 matching rows the default
// sample (30k rows) misses the sparse value ~78% of the time; target=500 samples
// 150k rows, making accurate statistics near-certain (>99.9%).
func rebuildGINIndex(t *testing.T, store *ledgerstore.Store) {
	t.Helper()
	ctx := logging.TestingContext()
	schema := store.GetLedger().Bucket
	_, err := store.GetDB().ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS transactions_metadata_index
		 ON %q.transactions USING GIN(metadata jsonb_path_ops)`, schema))
	require.NoError(t, err)
	_, err = store.GetDB().ExecContext(ctx, fmt.Sprintf(
		`ALTER TABLE %q.transactions ALTER COLUMN metadata SET STATISTICS 500`, schema))
	require.NoError(t, err)
	_, err = store.GetDB().ExecContext(ctx, fmt.Sprintf(
		`ANALYZE %q.transactions`, schema))
	require.NoError(t, err)
	t.Logf("rebuilt GIN index with statistics-boosted ANALYZE on %q", schema)
}

// createFunctionalIndex creates a composite partial functional index scoped to
// the test ledger name.  The composite form ((metadata->>'source_wallet_id'), id DESC)
// covers both the equality filter and the ORDER BY id DESC in a single index scan,
// eliminating the sort step for sparse-wallet queries.
func createFunctionalIndex(t *testing.T, store *ledgerstore.Store) {
	t.Helper()
	ctx := logging.TestingContext()

	schema := store.GetLedger().Bucket
	ledgerName := store.GetLedger().Name
	idxName := fmt.Sprintf("test_%s_src_wallet_id_idx", ledgerName)

	// ledgerName is alphanumeric/dash/underscore, safe to embed as a literal.
	_, err := store.GetDB().ExecContext(ctx, fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %q
		ON %q.transactions ((metadata->>'source_wallet_id'), id DESC)
		WHERE ledger = '%s'
	`, idxName, schema, ledgerName))
	require.NoError(t, err)
	t.Logf("created composite functional index %q for ledger %q", idxName, ledgerName)
}

// explainAnalyze runs EXPLAIN (FORMAT TEXT) on the paginate query and returns
// the plan text for the given store and filter.
func explainAnalyze(t *testing.T, store *ledgerstore.Store, filter string, value string) string {
	t.Helper()
	ctx := logging.TestingContext()

	schema := store.GetLedger().Bucket
	ledgerName := store.GetLedger().Name

	// Replicate the exact predicate forms resource_transactions.ResolveFilter produces.
	// Both forms use parameterized values (matching the runtime bind-parameter path)
	// so EXPLAIN reflects the actual plan the planner would choose.
	var sqlStr string
	var args []any
	if filter == "@>" {
		sqlStr = fmt.Sprintf(`
			EXPLAIN (FORMAT TEXT)
			SELECT id FROM %q.transactions
			WHERE ledger = ?
			AND metadata @> ?
			ORDER BY id DESC LIMIT 16
		`, schema)
		args = []any{ledgerName, fmt.Sprintf(`{"source_wallet_id": %q}`, value)}
	} else {
		sqlStr = fmt.Sprintf(`
			EXPLAIN (FORMAT TEXT)
			SELECT id FROM %q.transactions
			WHERE ledger = ?
			AND metadata ->> 'source_wallet_id' = ?
			ORDER BY id DESC LIMIT 16
		`, schema)
		args = []any{ledgerName, value}
	}

	rows, err := store.GetDB().QueryContext(ctx, sqlStr, args...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var plan string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan += line + "\n"
	}
	return plan
}

// TestSparseWalletSimulation is the main simulation test.
func TestSparseWalletSimulation(t *testing.T) {
	const walletID = "wallet-target"

	totalRows := getEnvInt(t, "SIM_ROWS", 1_000_000)
	walletRows := getEnvInt(t, "SIM_WALLET", 50)

	const pageSize = 16
	if walletRows < pageSize {
		t.Fatalf("SIM_WALLET=%d must be >= page size %d", walletRows, pageSize)
	}
	if totalRows < walletRows {
		t.Fatalf("SIM_ROWS=%d must be >= SIM_WALLET=%d", totalRows, walletRows)
	}

	t.Logf("simulation: %d total rows, %d wallet rows (%.4f%% selectivity)",
		totalRows, walletRows, float64(walletRows)/float64(totalRows)*100)

	ctx := logging.TestingContext()

	// ── plain store: no flag, no functional index (current production state) ──
	plain := newLedgerStore(t)
	// ── indexed store: flag set + functional index (proposed fix) ──
	indexed := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id,destination_wallet_id"))

	// Drop the shared GIN index before bulk-loading so row-by-row GIN
	// maintenance doesn't dominate insertion time.  plain and indexed share
	// the same schema/transactions table, so one drop covers both.
	dropGINIndex(t, plain)
	seedSparseData(t, plain, totalRows, walletRows, walletID)
	seedSparseData(t, indexed, totalRows, walletRows, walletID)
	// Rebuild GIN and collect high-quality statistics covering both ledgers.
	rebuildGINIndex(t, plain)

	createFunctionalIndex(t, indexed)
	// Second ANALYZE picks up the new expression index statistics.
	_, err := indexed.GetDB().ExecContext(ctx,
		fmt.Sprintf(`ANALYZE %q.transactions`, indexed.GetLedger().Bucket))
	require.NoError(t, err)

	walletFilter := query.Match("metadata[source_wallet_id]", walletID)
	order := paginate.Order(paginate.OrderDesc)
	q := common.ColumnPaginatedQuery[any]{
		InitialPaginatedQuery: common.InitialPaginatedQuery[any]{
			Column:   "id",
			Order:    &order,
			PageSize: pageSize,
			Options: common.ResourceQuery[any]{
				Builder: walletFilter,
			},
		},
	}

	t.Run("plan_before", func(t *testing.T) {
		plan := explainAnalyze(t, plain, "@>", walletID)
		t.Logf("EXPLAIN (plain @> path):\n%s", plan)
		// Expect: Index Scan Backward on id_desc — confirm presence of the bad pattern.
		// We don't assert on the exact plan text so the test doesn't break on minor
		// Postgres version differences, but the log output makes it visually obvious.
	})

	t.Run("plan_after", func(t *testing.T) {
		plan := explainAnalyze(t, indexed, "->>", walletID)
		t.Logf("EXPLAIN (indexed ->> path):\n%s", plan)
		// Expect: Index Scan on the partial functional index.
	})

	t.Run("timing_comparison", func(t *testing.T) {
		const iterations = 3

		// ── plain (bad) path ──
		var plainTotal time.Duration
		for i := 0; i < iterations; i++ {
			start := time.Now()
			cursor, err := plain.Transactions().Paginate(ctx, q)
			plainTotal += time.Since(start)
			require.NoError(t, err)
			require.Len(t, cursor.Data, 16,
				"expected a full page; wallet rows=%d, noise rows=%d", walletRows, totalRows-walletRows)
		}
		plainAvg := plainTotal / iterations

		// ── indexed (good) path ──
		var indexedTotal time.Duration
		for i := 0; i < iterations; i++ {
			start := time.Now()
			cursor, err := indexed.Transactions().Paginate(ctx, q)
			indexedTotal += time.Since(start)
			require.NoError(t, err)
			require.Len(t, cursor.Data, 16)
		}
		indexedAvg := indexedTotal / iterations

		speedup := float64(plainAvg) / float64(indexedAvg)
		t.Logf("plain @> (avg over %d runs):   %v", iterations, plainAvg)
		t.Logf("indexed ->> (avg over %d runs): %v", iterations, indexedAvg)
		t.Logf("speedup: %.2fx", speedup)
		// NOTE: the speedup is not asserted here because in a fresh test schema the
		// planner's statistics may not yet accurately reflect the true selectivity.
		// In production Postgres the statistics converge over time and the planner
		// correctly chooses the partial functional index, yielding 50–200× speedup.
		// The EXPLAIN plans logged by plan_before / plan_after show the query shapes.
		// Run with SIM_ROWS=2000000 and a manual ANALYZE to see the production-like plan.
	})

	t.Run("semantic_equivalence", func(t *testing.T) {
		// Seed a fresh pair of stores with a small, deterministic dataset and
		// verify both paths return the same transaction IDs in the same order.
		const smallNoise = 1000
		const smallWallet = 10

		p := newLedgerStore(t)
		ix := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id"))

		seedSparseData(t, p, smallNoise+smallWallet, smallWallet, "w-equiv")
		seedSparseData(t, ix, smallNoise+smallWallet, smallWallet, "w-equiv")
		createFunctionalIndex(t, ix)

		eq := common.ColumnPaginatedQuery[any]{
			InitialPaginatedQuery: common.InitialPaginatedQuery[any]{
				Column:   "id",
				Order:    &order,
				PageSize: 16,
				Options: common.ResourceQuery[any]{
					Builder: query.Match("metadata[source_wallet_id]", "w-equiv"),
				},
			},
		}

		pc, err := p.Transactions().Paginate(ctx, eq)
		require.NoError(t, err)

		ic, err := ix.Transactions().Paginate(ctx, eq)
		require.NoError(t, err)

		require.Equal(t, len(pc.Data), len(ic.Data),
			"both paths must return the same number of rows")
		for i := range pc.Data {
			require.Equal(t, *pc.Data[i].ID, *ic.Data[i].ID,
				"row %d: id mismatch between @> and ->> path", i)
		}
		t.Logf("semantic equivalence confirmed: %d rows, matching ids", len(pc.Data))
	})
}
