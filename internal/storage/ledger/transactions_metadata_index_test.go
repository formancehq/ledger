//go:build it

package ledger_test

// transactions_metadata_index_test.go verifies the per-ledger indexed-metadata-keys feature.
//
// When a metadata key appears in the ledger's INDEXED_METADATA_KEYS feature, the query builder
// must emit  metadata ->> 'key' = 'value'  instead of  metadata @> '{"key":"value"}'.
//
// The three properties we verify:
//
//  1. Flagged key returns correct rows (functional path produces right results).
//  2. Unflagged key still returns correct rows (containment path unchanged).
//  3. Semantic equivalence: a flagged-key query and a plain @> query on the same data
//     return identical row sets.

import (
	"math/big"
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
