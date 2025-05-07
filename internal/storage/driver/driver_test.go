//go:build it

package driver_test

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/query"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/common"
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

	// Create buckets first
	systemStore := system.New(db)
	for _, bucketName := range buckets {
		err := systemStore.CreateBucket(ctx, ledger.NewBucket(bucketName))
		if err != nil {
			// Ignore if bucket already exists
			if !strings.Contains(err.Error(), "bucket already exists") {
				require.NoError(t, err)
			}
		}
	}

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

func TestBucketDeletion(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
	)

	bucketName := "test-bucket-" + uuid.NewString()[:8]

	// Create the bucket first
	systemStore := system.New(db)
	err := systemStore.CreateBucket(ctx, ledger.NewBucket(bucketName))
	require.NoError(t, err)

	l1, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucketName,
	})
	require.NoError(t, err)
	_, err = d.CreateLedger(ctx, l1)
	require.NoError(t, err)

	l2, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucketName,
	})
	require.NoError(t, err)
	_, err = d.CreateLedger(ctx, l2)
	require.NoError(t, err)

	query := common.ColumnPaginatedQuery[any]{
		PageSize: 100,
	}
	bucketsResult, err := d.ListBucketsWithStatus(ctx, query)
	require.NoError(t, err)

	var foundBucket bool
	for _, b := range bucketsResult.Data {
		if b.Name == bucketName {
			foundBucket = true
			require.Nil(t, b.DeletedAt)
		}
	}
	require.True(t, foundBucket, "Bucket should be found in buckets list")

	err = d.MarkBucketAsDeleted(ctx, bucketName)
	require.NoError(t, err)

	bucketsResult, err = d.ListBucketsWithStatus(ctx, query)
	require.NoError(t, err)

	foundBucket = false
	for _, b := range bucketsResult.Data {
		if b.Name == bucketName {
			foundBucket = true
			require.NotNil(t, b.DeletedAt)
		}
	}
	require.True(t, foundBucket, "Bucket should be found in buckets list with deleted status")

	_, _, err = d.OpenLedger(ctx, l1.Name)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	err = d.RestoreBucket(ctx, bucketName)
	require.NoError(t, err)

	bucketsResult, err = d.ListBucketsWithStatus(ctx, query)
	require.NoError(t, err)

	foundBucket = false
	for _, b := range bucketsResult.Data {
		if b.Name == bucketName {
			foundBucket = true
			require.Nil(t, b.DeletedAt)
		}
	}
	require.True(t, foundBucket, "Bucket should be found in buckets list with restored status")

	_, _, err = d.OpenLedger(ctx, l1.Name)
	require.NoError(t, err)
}

func TestGetBucketsMarkedForDeletion(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
	)

	bucketName := "test-bucket-deletion-" + uuid.NewString()[:8]

	// Create the bucket first
	systemStore := system.New(db)
	err := systemStore.CreateBucket(ctx, ledger.NewBucket(bucketName))
	require.NoError(t, err)

	l, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucketName,
	})
	require.NoError(t, err)
	_, err = d.CreateLedger(ctx, l)
	require.NoError(t, err)

	err = d.MarkBucketAsDeleted(ctx, bucketName)
	require.NoError(t, err)

	deletedBuckets, err := d.GetBucketsMarkedForDeletion(ctx, -1) // -1 days means 1 day in the future
	require.NoError(t, err)
	require.NotContains(t, deletedBuckets, bucketName)

	deletedBuckets, err = d.GetBucketsMarkedForDeletion(ctx, 0) // 0 days means today
	require.NoError(t, err)
	require.Contains(t, deletedBuckets, bucketName)
}

func TestPhysicallyDeleteBucket(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
	)

	bucketName := "test-bucket-physical-" + uuid.NewString()[:8]

	// Create the bucket first
	systemStore := system.New(db)
	err := systemStore.CreateBucket(ctx, ledger.NewBucket(bucketName))
	require.NoError(t, err)

	l, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucketName,
	})
	require.NoError(t, err)
	_, err = d.CreateLedger(ctx, l)
	require.NoError(t, err)

	err = d.MarkBucketAsDeleted(ctx, bucketName)
	require.NoError(t, err)

	err = d.PhysicallyDeleteBucket(ctx, bucketName)
	require.NoError(t, err)

	query := common.ColumnPaginatedQuery[any]{
		PageSize: 100,
	}
	bucketsResult, err := d.ListBucketsWithStatus(ctx, query)
	require.NoError(t, err)

	for _, b := range bucketsResult.Data {
		require.NotEqual(t, bucketName, b.Name, "Bucket should not be found after physical deletion")
	}
}
