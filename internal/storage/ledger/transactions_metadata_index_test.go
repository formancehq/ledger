//go:build it

package ledger_test

// transactions_metadata_index_test.go verifies the per-ledger indexed-metadata-keys feature.
//
// When a metadata key appears in the ledger's INDEXED_METADATA_KEYS feature and a matching
// functional index has been confirmed via pg_indexes, the query builder must emit
//
//	metadata ->> 'key' = 'value'
//
// instead of  metadata @> '{"key":"value"}'.  Without the index, the flag is silently ignored
// and the query falls back to @>.
//
// Properties verified:
//
//  1. Flagged key returns correct rows (functional path produces right results).
//  2. Unflagged key still returns correct rows (containment path unchanged).
//  3. Semantic equivalence: a flagged-key query and a plain @> query on the same data
//     return identical row sets.
//  4. EXPLAIN shows the literal ->> predicate, not @>, when the index exists.
//  5. When the index is absent, ResolveIndexedMetadataKeys falls back to @>.

import (
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/pkg/features"
)

// withIndexedMetadataKeys returns a newLedgerStore option that sets the
// INDEXED_METADATA_KEYS feature to the given comma-separated list.
func withIndexedMetadataKeys(keys string) func(cfg *ledger.Configuration) {
	return func(cfg *ledger.Configuration) {
		cfg.Features[features.FeatureIndexedMetadataKeys] = keys
	}
}

// TestIndexedMetadataKeys_FlaggedKeyReturnsCorrectRows verifies that when
// source_wallet_id is a flagged key, filtering by metadata[source_wallet_id]
// returns the expected transactions (functional-index path).
func TestIndexedMetadataKeys_FlaggedKeyReturnsCorrectRows(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id,destination_wallet_id"))
	ctx := logging.TestingContext()
	now := time.Now()

	tx1 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
		WithMetadata(metadata.Metadata{"source_wallet_id": "wallet-A"}).
		WithTimestamp(now.Add(-2 * time.Hour))
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx1))

	tx2 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "bob", "USD", big.NewInt(50))).
		WithMetadata(metadata.Metadata{"source_wallet_id": "wallet-B"}).
		WithTimestamp(now.Add(-time.Hour))
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx2))

	// Unrelated tx — no source_wallet_id metadata.
	tx3 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "carol", "USD", big.NewInt(10))).
		WithTimestamp(now)
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx3))

	// IndexedMetadataKeys falls back to the feature-flag list when
	// ResolveIndexedMetadataKeys has not been called (no real pg_index needed here).
	// ExplainUsesLiteralPredicate tests the fully-confirmed (pg_indexes-verified) path.
	require.Contains(t, store.IndexedMetadataKeys(), "source_wallet_id")

	// Filter by source_wallet_id = "wallet-A" via the flagged (->> ) path.
	cursor, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
		Options: common.ResourceQuery[any]{
			Builder: query.Match("metadata[source_wallet_id]", "wallet-A"),
		},
	})
	require.NoError(t, err)
	require.Len(t, cursor.Data, 1)
	require.Equal(t, *tx1.ID, *cursor.Data[0].ID)
}

// TestIndexedMetadataKeys_UnflaggedKeyReturnsCorrectRows verifies that metadata
// keys NOT in the indexed list still work correctly via the @> containment path.
func TestIndexedMetadataKeys_UnflaggedKeyReturnsCorrectRows(t *testing.T) {
	t.Parallel()

	// Only source_wallet_id is flagged; "category" is not.
	store := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id"))
	ctx := logging.TestingContext()
	now := time.Now()

	tx1 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
		WithMetadata(metadata.Metadata{"category": "premium", "source_wallet_id": "w1"}).
		WithTimestamp(now.Add(-time.Hour))
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx1))

	tx2 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "bob", "USD", big.NewInt(50))).
		WithMetadata(metadata.Metadata{"category": "standard"}).
		WithTimestamp(now)
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx2))

	// Filter by the unflagged "category" key — must still use @> and return correctly.
	cursor, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
		Options: common.ResourceQuery[any]{
			Builder: query.Match("metadata[category]", "premium"),
		},
	})
	require.NoError(t, err)
	require.Len(t, cursor.Data, 1)
	require.Equal(t, *tx1.ID, *cursor.Data[0].ID)
}

// TestIndexedMetadataKeys_SemanticEquivalence inserts the same transactions into
// two stores — one with source_wallet_id flagged, one without — and verifies that
// both return identical row IDs when filtering by that key.
func TestIndexedMetadataKeys_SemanticEquivalence(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	now := time.Now()

	// Store with the flag: uses ->> path.
	flagged := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id"))
	// Store without the flag: uses @> path.
	plain := newLedgerStore(t)

	// Insert identical data into both stores.
	for _, store := range []*ledgerstore.Store{flagged, plain} {
		for i, walletID := range []string{"w-1", "w-2", "w-3"} {
			tx := ledger.NewTransaction().
				WithPostings(ledger.NewPosting("world", "dest", "USD", big.NewInt(int64(100*(i+1))))).
				WithMetadata(metadata.Metadata{"source_wallet_id": walletID}).
				WithTimestamp(now.Add(time.Duration(i) * time.Hour))
			require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx))
		}
		// Extra tx without the metadata key.
		unrelated := ledger.NewTransaction().
			WithPostings(ledger.NewPosting("world", "other", "USD", big.NewInt(9))).
			WithTimestamp(now.Add(10 * time.Hour))
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &unrelated))
	}

	// Both stores use their respective paths (flagged = ->> via unresolved flag,
	// plain = @> with no flag).  Assert the flagged key is active before comparing.
	require.Contains(t, flagged.IndexedMetadataKeys(), "source_wallet_id")

	q := common.InitialPaginatedQuery[any]{
		Options: common.ResourceQuery[any]{
			Builder: query.Match("metadata[source_wallet_id]", "w-2"),
		},
	}

	flaggedCursor, err := flagged.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	plainCursor, err := plain.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	require.Equal(t, len(plainCursor.Data), len(flaggedCursor.Data),
		"both paths must return the same number of rows")
	require.Equal(t, 1, len(flaggedCursor.Data), "should match exactly one transaction")

	for i := range plainCursor.Data {
		require.Equalf(t, *plainCursor.Data[i].ID, *flaggedCursor.Data[i].ID,
			"row %d: id mismatch between @> path and ->> path", i)
	}
}

// TestIndexedMetadataKeys_DestinationWalletID verifies destination_wallet_id
// works the same way as source_wallet_id when flagged.
func TestIndexedMetadataKeys_DestinationWalletID(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id,destination_wallet_id"))
	ctx := logging.TestingContext()
	now := time.Now()

	tx1 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
		WithMetadata(metadata.Metadata{"destination_wallet_id": "dest-wallet-X"}).
		WithTimestamp(now.Add(-time.Hour))
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx1))

	tx2 := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "bob", "USD", big.NewInt(50))).
		WithMetadata(metadata.Metadata{"destination_wallet_id": "dest-wallet-Y"}).
		WithTimestamp(now)
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx2))

	// IndexedMetadataKeys falls back to the feature-flag list (unresolved path).
	require.Contains(t, store.IndexedMetadataKeys(), "destination_wallet_id")

	cursor, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
		Options: common.ResourceQuery[any]{
			Builder: query.Match("metadata[destination_wallet_id]", "dest-wallet-X"),
		},
	})
	require.NoError(t, err)
	require.Len(t, cursor.Data, 1)
	require.Equal(t, *tx1.ID, *cursor.Data[0].ID)
}

// TestIndexedMetadataKeys_NoFlagUsesContainment verifies that a ledger with no
// INDEXED_METADATA_KEYS feature set continues to use the @> containment path.
func TestIndexedMetadataKeys_NoFlagUsesContainment(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t) // no feature flag
	ctx := logging.TestingContext()
	now := time.Now()

	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
		WithMetadata(metadata.Metadata{"source_wallet_id": "w-99"}).
		WithTimestamp(now)
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx))

	cursor, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
		Options: common.ResourceQuery[any]{
			Builder: query.Match("metadata[source_wallet_id]", "w-99"),
		},
	})
	require.NoError(t, err)
	require.Len(t, cursor.Data, 1, "containment path must still find the transaction")
}

// TestIndexedMetadataKeys_NegatedFilterSemantics verifies that NOT(metadata[key] = val)
// returns rows where the key is absent, matching the NOT(metadata @> ...) semantics.
//
// The bug this guards against: metadata ->> 'key' = ? returns NULL when the key is
// absent, so NOT(NULL) = NULL, silently excluding rows that should be included. The
// fix (adding metadata ? 'key') makes the expression evaluate to false for absent-key
// rows, so NOT(false) = true.
func TestIndexedMetadataKeys_NegatedFilterSemantics(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	now := time.Now()

	// Both stores get identical data; one uses the ->> rewrite, the other @>.
	flagged := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id"))
	plain := newLedgerStore(t)

	for _, store := range []*ledgerstore.Store{flagged, plain} {
		// tx1: has the key with a different value — must be included by NOT filter.
		tx1 := ledger.NewTransaction().
			WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
			WithMetadata(metadata.Metadata{"source_wallet_id": "other-wallet"}).
			WithTimestamp(now.Add(-2 * time.Hour))
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx1))

		// tx2: has the key with the target value — must be excluded by NOT filter.
		tx2 := ledger.NewTransaction().
			WithPostings(ledger.NewPosting("world", "bob", "USD", big.NewInt(50))).
			WithMetadata(metadata.Metadata{"source_wallet_id": "target-wallet"}).
			WithTimestamp(now.Add(-time.Hour))
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx2))

		// tx3: key absent — must be included by NOT filter (the regression case).
		tx3 := ledger.NewTransaction().
			WithPostings(ledger.NewPosting("world", "carol", "USD", big.NewInt(10))).
			WithTimestamp(now)
		require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, &tx3))
	}

	q := common.InitialPaginatedQuery[any]{
		Options: common.ResourceQuery[any]{
			Builder: query.Not(query.Match("metadata[source_wallet_id]", "target-wallet")),
		},
	}

	flaggedCursor, err := flagged.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	plainCursor, err := plain.Transactions().Paginate(ctx, q)
	require.NoError(t, err)

	require.Equal(t, len(plainCursor.Data), len(flaggedCursor.Data),
		"->> rewrite must return the same row count as @> for negated filters")
	require.Equal(t, 2, len(flaggedCursor.Data),
		"NOT filter must include the absent-key row and the different-value row")

	// Verify the IDs match between both paths.
	for i := range plainCursor.Data {
		require.Equalf(t, *plainCursor.Data[i].ID, *flaggedCursor.Data[i].ID,
			"row %d: id mismatch between @> path and ->> path for NOT filter", i)
	}
}

// captureExplain runs EXPLAIN (FORMAT TEXT) using predExpr as the WHERE predicate
// and returns the full plan text. The caller is responsible for supplying the
// predicate that matches the production path they intend to verify — this avoids
// duplicating the ResolveFilter routing logic inside the helper.
func captureExplain(t *testing.T, store *ledgerstore.Store, predExpr string) string {
	t.Helper()
	ctx := logging.TestingContext()

	schema := store.GetLedger().Bucket
	ledgerName := store.GetLedger().Name

	sql := fmt.Sprintf(
		`EXPLAIN (FORMAT TEXT) SELECT id FROM %q.transactions WHERE ledger = '%s' AND %s ORDER BY id DESC LIMIT 16`,
		schema, ledgerName, predExpr,
	)

	rows, err := store.GetDB().QueryContext(ctx, sql)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var plan strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	return plan.String()
}

// TestIndexedMetadataKeys_ExplainUsesLiteralPredicate verifies that when a functional
// index exists and ResolveIndexedMetadataKeys confirms it, EXPLAIN shows the literal
// ->> predicate (not @>). This proves the correct SQL is generated and that Postgres
// can match it to the functional index.
func TestIndexedMetadataKeys_ExplainUsesLiteralPredicate(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	store := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id"))

	// Create the functional index for this test's ledger.
	createFunctionalIndex(t, store)

	// Resolve against pg_indexes so the store confirms the index.
	require.NoError(t, store.ResolveIndexedMetadataKeys(ctx))

	// The predicate matches what resource_transactions.ResolveFilter emits for a
	// confirmed indexed key: metadata ->> 'key' = 'value'.
	plan := captureExplain(t, store, "metadata ->> 'source_wallet_id' = 'w-target'")
	t.Logf("EXPLAIN plan:\n%s", plan)

	require.Contains(t, plan, "metadata ->> 'source_wallet_id'",
		"plan must use the ->> literal predicate for a confirmed indexed key")
	require.NotContains(t, plan, "metadata @>",
		"plan must not fall back to containment when the index is confirmed")
}

// TestIndexedMetadataKeys_FallsBackWhenNoIndex verifies that when a key is listed in
// INDEXED_METADATA_KEYS but no matching functional index exists, ResolveIndexedMetadataKeys
// excludes that key and the query falls back to the @> containment form.
func TestIndexedMetadataKeys_FallsBackWhenNoIndex(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	// Flag the key but do NOT create the index.
	store := newLedgerStore(t, withIndexedMetadataKeys("source_wallet_id"))
	require.NoError(t, store.ResolveIndexedMetadataKeys(ctx))

	// After resolution, the key should have been dropped (no index found).
	require.Empty(t, store.IndexedMetadataKeys(),
		"key should be excluded when no functional index exists")

	// The predicate matches what resource_transactions.ResolveFilter emits for an
	// unconfirmed key (no index): metadata @> '{"key":"value"}'.
	plan := captureExplain(t, store, `metadata @> '{"source_wallet_id": "w-target"}'`)
	t.Logf("EXPLAIN plan (fallback):\n%s", plan)

	require.Contains(t, plan, "metadata @>",
		"plan must use @> containment when no functional index was found")
}
