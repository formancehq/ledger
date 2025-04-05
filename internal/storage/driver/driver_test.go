//go:build it

package driver_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLedgersCreate(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
	)

	buckets := []string{"bucket1", "bucket2"}
	const countLedgers = 30

	wg := sync.WaitGroup{}
	wg.Add(countLedgers)
	errors := make(chan error, countLedgers)
	for i := range countLedgers {
		go func() {
			defer wg.Done()

			l, err := ledger.New(fmt.Sprintf("ledger%d", i), ledger.Configuration{
				Bucket: buckets[rand.Int31n(int32(len(buckets)))],
			})
			if err != nil {
				errors <- err
				return
			}

			_, err = d.CreateLedger(ctx, l)
			if err != nil {
				errors <- err
				return
			}
		}()
	}
	wg.Wait()

	close(errors)

	for err := range errors {
		require.NoError(t, err)
	}

	hasReachMinimalVersion, err := d.HasReachMinimalVersion(ctx)
	require.NoError(t, err)
	require.True(t, hasReachMinimalVersion)

	err = d.UpgradeAllBuckets(ctx)
	require.NoError(t, err)
}

func TestLedgersList(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
	)

	bucket := uuid.NewString()[:8]

	l1, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucket,
	})
	require.NoError(t, err)

	_, err = d.CreateLedger(ctx, l1)
	require.NoError(t, err)

	l2, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucket,
	})
	require.NoError(t, err)

	_, err = d.CreateLedger(ctx, l2)
	require.NoError(t, err)

	q := ledgercontroller.NewListLedgersQuery(15)
	q.Options.Builder = query.Match("bucket", bucket)

	cursor, err := d.ListLedgers(ctx, q)
	require.NoError(t, err)

	require.Len(t, cursor.Data, 2)
}

func TestLedgerUpdateMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
	)

	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	addedMetadata := metadata.Metadata{
		"foo": "bar",
	}
	err = d.UpdateLedgerMetadata(ctx, l.Name, addedMetadata)
	require.NoError(t, err)
}

func TestLedgerDeleteMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
	)

	l := ledger.MustNewWithDefault(uuid.NewString()).WithMetadata(metadata.Metadata{
		"foo": "bar",
	})
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	err = d.DeleteLedgerMetadata(ctx, l.Name, "foo")
	require.NoError(t, err)
}

func TestMarkBucketAsDeleted(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(db, ledgerstore.NewFactory(db), bucket.NewDefaultFactory())

	// Create a unique bucket name
	bucketName := uuid.NewString()[:8]

	// Create multiple ledgers in the bucket
	ledgers := make([]*ledger.Ledger, 3)
	for i := 0; i < 3; i++ {
		l, err := ledger.New(uuid.NewString(), ledger.Configuration{
			Bucket: bucketName,
		})
		require.NoError(t, err)

		_, err = d.CreateLedger(ctx, l)
		require.NoError(t, err)
		ledgers[i] = l
	}

	// Create a ledger in a different bucket
	otherBucketName := uuid.NewString()[:8]
	otherLedger, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: otherBucketName,
	})
	require.NoError(t, err)
	_, err = d.CreateLedger(ctx, otherLedger)
	require.NoError(t, err)

	// Mark the first bucket as deleted
	err = d.MarkBucketAsDeleted(ctx, bucketName)
	require.NoError(t, err)

	// Verify all ledgers in the bucket are marked as deleted
	for _, l := range ledgers {
		ledgerFromDB, err := d.GetLedger(ctx, l.Name)
		require.Error(t, err) // Should return error as ledger is deleted
		require.Nil(t, ledgerFromDB)
	}

	// Verify the ledger in the other bucket is still accessible
	ledgerFromDB, err := d.GetLedger(ctx, otherLedger.Name)
	require.NoError(t, err)
	require.NotNil(t, ledgerFromDB)
	require.Equal(t, otherLedger.Name, ledgerFromDB.Name)
	require.Equal(t, otherBucketName, ledgerFromDB.Bucket)
	require.Nil(t, ledgerFromDB.DeletedAt)

	// Try to mark a non-existent bucket as deleted
	err = d.MarkBucketAsDeleted(ctx, "nonexistent-bucket")
	require.NoError(t, err) // Should not return error for non-existent bucket
}
