//go:build it

package driver_test

import (
	"math/big"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/pkg/features"
)

func TestFreshBucketRevertPreservesTransactionMetadataHistory(t *testing.T) {
	t.Parallel()
	testRevertPreservesTransactionMetadataHistory(t, false)
}

func TestExistingBucketSecondLedgerRevertPreservesTransactionMetadataHistory(t *testing.T) {
	t.Parallel()
	testRevertPreservesTransactionMetadataHistory(t, true)
}

func testRevertPreservesTransactionMetadataHistory(t *testing.T, existingBucket bool) {
	t.Helper()
	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)
	db, err := connect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)
	require.NoError(t, systemstore.Migrate(ctx, db))
	d := driver.New(db, ledgerstore.NewFactory(db), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())
	bucketName := "revert_" + uuid.NewString()[:8]

	if existingBucket {
		// Initialize the bucket through a first ledger before creating the tested ledger.
		first, err := ledger.New(uuid.NewString(), ledger.Configuration{Bucket: bucketName})
		require.NoError(t, err)
		_, err = d.CreateLedger(ctx, first)
		require.NoError(t, err)
	}

	l, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucketName,
		Features: features.FeatureSet{
			features.FeatureTransactionMetadataHistory: "SYNC",
		},
	})
	require.NoError(t, err)
	store, err := d.CreateLedger(ctx, l)
	require.NoError(t, err)

	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
		WithMetadata(metadata.Metadata{"order": "123"})
	require.NoError(t, store.InsertTransaction(ctx, &tx))
	require.NotNil(t, tx.ID)

	// There is no history endpoint; inspect the persisted history for this transaction.
	history := func() []metadata.Metadata {
		t.Helper()
		var rows []struct {
			Metadata metadata.Metadata `bun:"metadata"`
		}
		err := db.NewSelect().
			TableExpr(store.GetPrefixedRelationName("transactions_metadata")).
			Column("metadata").
			Where("ledger = ?", l.Name).
			Where("transactions_id = ?", *tx.ID).
			OrderExpr("revision ASC").
			Scan(ctx, &rows)
		require.NoError(t, err)
		revisions := make([]metadata.Metadata, len(rows))
		for i, row := range rows {
			revisions[i] = row.Metadata
		}
		return revisions
	}
	require.Equal(t, []metadata.Metadata{{"order": "123"}}, history())

	// A real metadata change must still append a revision.
	_, modified, err := store.UpdateTransactionMetadata(ctx, *tx.ID, metadata.Metadata{"order": "456"}, time.Time{})
	require.NoError(t, err)
	require.True(t, modified)
	beforeRevert := history()
	require.Equal(t, []metadata.Metadata{{"order": "123"}, {"order": "456"}}, beforeRevert)

	revertedTx, reverted, err := store.RevertTransaction(ctx, *tx.ID, time.Time{})
	require.NoError(t, err)
	require.True(t, reverted)
	require.NotNil(t, revertedTx)
	require.True(t, revertedTx.IsReverted())
	require.Equal(t, metadata.Metadata{"order": "456"}, revertedTx.Metadata)
	require.Equal(t, beforeRevert, history(), "reverting without changing metadata must preserve metadata history")
}
