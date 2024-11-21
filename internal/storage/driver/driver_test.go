package driver_test

import (
	"context"
	"errors"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/pointer"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"testing"
	"time"
)

func TestUpgradeAllLedgers(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("single bucket with no error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		ledgerStoreFactory := driver.NewLedgerStoreFactory(ctrl)
		bucketFactory := driver.NewBucketFactory(ctrl)
		systemStore := driver.NewSystemStore(ctrl)

		d := driver.New(
			ledgerStoreFactory,
			systemStore,
			bucketFactory,
		)

		bucket := driver.NewMockBucket(ctrl)

		systemStore.EXPECT().
			GetDistinctBuckets(gomock.Any()).
			Return([]string{ledger.DefaultBucket}, nil)

		bucketFactory.EXPECT().
			Create(ledger.DefaultBucket).
			AnyTimes().
			Return(bucket)

		bucket.EXPECT().
			Migrate(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, minimalVersionReached chan struct{}, opts ...migrations.Option) error {
				close(minimalVersionReached)
				return nil
			})

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		t.Cleanup(cancel)

		require.NoError(t, d.UpgradeAllBuckets(ctx, make(chan struct{})))
	})

	t.Run("with concurrent buckets", func(t *testing.T) {
		t.Parallel()

		t.Run("and no error", func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			ledgerStoreFactory := driver.NewLedgerStoreFactory(ctrl)
			bucketFactory := driver.NewBucketFactory(ctrl)
			systemStore := driver.NewSystemStore(ctrl)

			d := driver.New(
				ledgerStoreFactory,
				systemStore,
				bucketFactory,
			)

			bucketList := []string{"bucket1", "bucket2", "bucket3"}
			buckets := make(map[string]bucket.Bucket)

			for _, name := range bucketList {
				bucket := driver.NewMockBucket(ctrl)
				buckets[name] = bucket

				bucketFactory.EXPECT().
					Create(name).
					Return(bucket)

				bucket.EXPECT().
					Migrate(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, minimalVersionReached chan struct{}, opts ...migrations.Option) error {
						close(minimalVersionReached)
						return nil
					})
			}

			systemStore.EXPECT().
				GetDistinctBuckets(gomock.Any()).
				Return(bucketList, nil)

			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			t.Cleanup(cancel)

			require.NoError(t, d.UpgradeAllBuckets(ctx, make(chan struct{})))
		})

		t.Run("and error", func(t *testing.T) {
			t.Parallel()

			//ctx := context.Background()

			ctx := logging.ContextWithLogger(ctx, logging.NewLogrus(logrus.New()))

			ctrl := gomock.NewController(t)
			ledgerStoreFactory := driver.NewLedgerStoreFactory(ctrl)
			bucketFactory := driver.NewBucketFactory(ctrl)
			systemStore := driver.NewSystemStore(ctrl)

			d := driver.New(ledgerStoreFactory, systemStore, bucketFactory)

			bucket1 := driver.NewMockBucket(ctrl)
			bucket2 := driver.NewMockBucket(ctrl)
			bucketList := []string{"bucket1", "bucket2"}
			allBucketsMinimalVersionReached := make(chan struct{})

			bucketFactory.EXPECT().
				Create(gomock.AnyOf(
					gomock.Eq("bucket1"),
					gomock.Eq("bucket2"),
				)).
				AnyTimes().
				DoAndReturn(func(name string) bucket.Bucket {
					if name == "bucket1" {
						return bucket1
					}
					return bucket2
				})

			bucket1MigrationStarted := make(chan struct{})
			bucket1.EXPECT().
				Migrate(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes().
				DoAndReturn(func(ctx context.Context, minimalVersionReached chan struct{}, opts ...migrations.Option) error {
					close(minimalVersionReached)
					close(bucket1MigrationStarted)

					return nil
				})

			firstCall := true
			bucket2.EXPECT().
				Migrate(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes().
				DoAndReturn(func(ctx context.Context, minimalVersionReached chan struct{}, opts ...migrations.Option) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-bucket1MigrationStarted:
						if firstCall {
							firstCall = false
							return errors.New("unknown error")
						}
						close(minimalVersionReached)
						return nil
					}
				})

			systemStore.EXPECT().
				GetDistinctBuckets(gomock.Any()).
				AnyTimes().
				Return(bucketList, nil)

			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			t.Cleanup(cancel)

			bucket1MigrationStarted = make(chan struct{})
			err := d.UpgradeAllBuckets(ctx, allBucketsMinimalVersionReached)
			require.NoError(t, err)
		})
	})
}

func TestLedgersCreate(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	ctrl := gomock.NewController(t)
	ledgerStoreFactory := driver.NewLedgerStoreFactory(ctrl)
	bucketFactory := driver.NewBucketFactory(ctrl)
	systemStore := driver.NewSystemStore(ctrl)

	d := driver.New(
		ledgerStoreFactory,
		systemStore,
		bucketFactory,
	)

	l := pointer.For(ledger.MustNewWithDefault("test"))

	bucket := driver.NewMockBucket(ctrl)
	bucketFactory.EXPECT().
		Create(ledger.DefaultBucket).
		Return(bucket)

	systemStore.EXPECT().
		CreateLedger(gomock.Any(), l)

	bucket.EXPECT().
		Migrate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	bucket.EXPECT().
		AddLedger(gomock.Any(), *l).
		Return(nil)

	ledgerStoreFactory.EXPECT().
		Create(gomock.Any(), *l).
		Return(&ledgerstore.Store{})

	_, err := d.CreateLedger(ctx, l)
	require.NoError(t, err)
}

func TestLedgersList(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	ctrl := gomock.NewController(t)
	ledgerStoreFactory := driver.NewLedgerStoreFactory(ctrl)
	bucketFactory := driver.NewBucketFactory(ctrl)
	systemStore := driver.NewSystemStore(ctrl)

	driver := driver.New(
		ledgerStoreFactory,
		systemStore,
		bucketFactory,
	)

	query := ledgercontroller.NewListLedgersQuery(15)

	systemStore.EXPECT().
		ListLedgers(gomock.Any(), query).
		Return(&bunpaginate.Cursor[ledger.Ledger]{
			Data: []ledger.Ledger{
				ledger.MustNewWithDefault("testing"),
			},
		}, nil)

	cursor, err := driver.ListLedgers(ctx, query)
	require.NoError(t, err)
	require.Len(t, cursor.Data, 1)
}

func TestLedgerUpdateMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	ctrl := gomock.NewController(t)
	ledgerStoreFactory := driver.NewLedgerStoreFactory(ctrl)
	bucketFactory := driver.NewBucketFactory(ctrl)
	systemStore := driver.NewSystemStore(ctrl)

	d := driver.New(
		ledgerStoreFactory,
		systemStore,
		bucketFactory,
	)

	l := ledger.MustNewWithDefault(uuid.NewString())

	addedMetadata := metadata.Metadata{
		"foo": "bar",
	}
	systemStore.EXPECT().
		UpdateLedgerMetadata(gomock.Any(), l.Name, addedMetadata).
		Return(nil)

	err := d.UpdateLedgerMetadata(ctx, l.Name, addedMetadata)
	require.NoError(t, err)
}

func TestLedgerDeleteMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	ledgerStoreFactory := driver.NewLedgerStoreFactory(ctrl)
	bucketFactory := driver.NewBucketFactory(ctrl)
	systemStore := driver.NewSystemStore(ctrl)

	d := driver.New(
		ledgerStoreFactory,
		systemStore,
		bucketFactory,
	)

	l := ledger.MustNewWithDefault(uuid.NewString()).WithMetadata(metadata.Metadata{
		"foo": "bar",
	})

	systemStore.EXPECT().
		DeleteLedgerMetadata(gomock.Any(), l.Name, "foo").
		Return(nil)

	err := d.DeleteLedgerMetadata(ctx, l.Name, "foo")
	require.NoError(t, err)
}
