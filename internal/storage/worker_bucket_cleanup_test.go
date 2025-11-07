//go:build it

package storage

import (
	"os"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/bun/bundebug"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	. "github.com/formancehq/go-libs/v3/testing/utils"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestBucketCleanupRunner(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a schedule that runs immediately
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse("* * * * * *") // Every second
	require.NoError(t, err)

	// Create runner with short retention period for testing
	retentionPeriod := 1 * time.Hour

	t.Run("should not delete buckets that are not deleted", func(t *testing.T) {
		t.Parallel()

		store, db := newStoreAndDBForWorkerTest(t)
		runner := NewBucketCleanupRunner(
			logging.Testing(),
			db,
			BucketCleanupRunnerConfig{
				RetentionPeriod: retentionPeriod,
				Schedule:        schedule,
			},
		)

		bucketName := "test-bucket-not-deleted"
		ledgerName := "ledger1"

		// Create a ledger in a bucket
		l, err := ledger.New(ledgerName, ledger.Configuration{
			Bucket: bucketName,
		})
		require.NoError(t, err)
		require.NoError(t, store.CreateLedger(ctx, l))

		// Create the bucket schema
		bucketMigrator := bucket.GetMigrator(db, bucketName)
		require.NoError(t, bucketMigrator.Up(ctx))

		// Run the cleanup
		require.NoError(t, runner.run(ctx))

		// Verify the ledger still exists
		_, err = store.GetLedger(ctx, ledgerName)
		require.NoError(t, err)
	})

	t.Run("should not delete recently deleted buckets", func(t *testing.T) {
		t.Parallel()

		store, db := newStoreAndDBForWorkerTest(t)
		runner := NewBucketCleanupRunner(
			logging.Testing(),
			db,
			BucketCleanupRunnerConfig{
				RetentionPeriod: retentionPeriod,
				Schedule:        schedule,
			},
		)

		bucketName := "test-bucket-recently-deleted"
		ledgerName := "ledger2"

		// Create a ledger in a bucket
		l, err := ledger.New(ledgerName, ledger.Configuration{
			Bucket: bucketName,
		})
		require.NoError(t, err)
		require.NoError(t, store.CreateLedger(ctx, l))

		// Create the bucket schema
		bucketMigrator := bucket.GetMigrator(db, bucketName)
		require.NoError(t, bucketMigrator.Up(ctx))

		// Soft delete the bucket (just now)
		require.NoError(t, store.DeleteBucket(ctx, bucketName))

		// Run the cleanup
		require.NoError(t, runner.run(ctx))

		// Verify the ledger still exists in _system.ledgers (with deleted_at set)
		var deletedLedger ledger.Ledger
		err = db.NewSelect().
			Model(&deletedLedger).
			Where("name = ?", ledgerName).
			Scan(ctx)
		require.NoError(t, err)
		require.NotNil(t, deletedLedger.DeletedAt, "ledger should have deleted_at set")
	})

	t.Run("should hard delete old deleted buckets", func(t *testing.T) {
		t.Parallel()

		store, db := newStoreAndDBForWorkerTest(t)
		runner := NewBucketCleanupRunner(
			logging.Testing(),
			db,
			BucketCleanupRunnerConfig{
				RetentionPeriod: retentionPeriod,
				Schedule:        schedule,
			},
		)

		bucketName := "test-bucket-old-deleted"
		ledgerName := "ledger3"

		// Create a ledger in a bucket
		l, err := ledger.New(ledgerName, ledger.Configuration{
			Bucket: bucketName,
		})
		require.NoError(t, err)
		require.NoError(t, store.CreateLedger(ctx, l))

		// Create the bucket schema
		bucketMigrator := bucket.GetMigrator(db, bucketName)
		require.NoError(t, bucketMigrator.Up(ctx))

		// Soft delete the bucket
		require.NoError(t, store.DeleteBucket(ctx, bucketName))

		// Manually set deleted_at to be older than retention period
		oldTime := time.Now().Add(-2 * retentionPeriod)
		_, err = db.NewUpdate().
			Model(&ledger.Ledger{}).
			Set("deleted_at = ?", oldTime).
			Where("bucket = ?", bucketName).
			Exec(ctx)
		require.NoError(t, err)

		// Run the cleanup
		require.NoError(t, runner.run(ctx))

		// Verify the ledger is deleted from _system.ledgers
		count, err := db.NewSelect().
			Model(&ledger.Ledger{}).
			Where("bucket = ?", bucketName).
			Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, count, "ledger should be deleted from _system.ledgers")
	})

	t.Run("should handle multiple buckets correctly", func(t *testing.T) {
		t.Parallel()

		store, db := newStoreAndDBForWorkerTest(t)
		runner := NewBucketCleanupRunner(
			logging.Testing(),
			db,
			BucketCleanupRunnerConfig{
				RetentionPeriod: retentionPeriod,
				Schedule:        schedule,
			},
		)

		bucket1Name := "test-bucket-multi-1"
		bucket2Name := "test-bucket-multi-2"
		bucket3Name := "test-bucket-multi-3"
		ledger1Name := "ledger-multi-1"
		ledger2Name := "ledger-multi-2"
		ledger3Name := "ledger-multi-3"

		// Create ledgers in different buckets
		l1, err := ledger.New(ledger1Name, ledger.Configuration{Bucket: bucket1Name})
		require.NoError(t, err)
		l2, err := ledger.New(ledger2Name, ledger.Configuration{Bucket: bucket2Name})
		require.NoError(t, err)
		l3, err := ledger.New(ledger3Name, ledger.Configuration{Bucket: bucket3Name})
		require.NoError(t, err)

		require.NoError(t, store.CreateLedger(ctx, l1))
		require.NoError(t, store.CreateLedger(ctx, l2))
		require.NoError(t, store.CreateLedger(ctx, l3))

		// Create bucket schemas
		require.NoError(t, bucket.GetMigrator(db, bucket1Name).Up(ctx))
		require.NoError(t, bucket.GetMigrator(db, bucket2Name).Up(ctx))
		require.NoError(t, bucket.GetMigrator(db, bucket3Name).Up(ctx))

		// Delete all buckets
		require.NoError(t, store.DeleteBucket(ctx, bucket1Name))
		require.NoError(t, store.DeleteBucket(ctx, bucket2Name))
		require.NoError(t, store.DeleteBucket(ctx, bucket3Name))

		// Set bucket1 to be old (should be deleted)
		oldTime := time.Now().Add(-2 * retentionPeriod)
		_, err = db.NewUpdate().
			Model(&ledger.Ledger{}).
			Set("deleted_at = ?", oldTime).
			Where("bucket = ?", bucket1Name).
			Exec(ctx)
		require.NoError(t, err)

		// Set bucket2 to be old (should be deleted)
		_, err = db.NewUpdate().
			Model(&ledger.Ledger{}).
			Set("deleted_at = ?", oldTime).
			Where("bucket = ?", bucket2Name).
			Exec(ctx)
		require.NoError(t, err)

		// bucket3 remains recently deleted (should NOT be deleted)

		// Run the cleanup
		require.NoError(t, runner.run(ctx))

		// Verify ledgers from bucket1 and bucket2 are deleted
		count, err := db.NewSelect().
			Model(&ledger.Ledger{}).
			Where("bucket = ?", bucket1Name).
			Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, count, "ledger1 should be deleted")

		count, err = db.NewSelect().
			Model(&ledger.Ledger{}).
			Where("bucket = ?", bucket2Name).
			Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, count, "ledger2 should be deleted")

		// Verify ledger3 still exists in DB with deleted_at (soft deleted, not hard deleted)
		var ledger3 ledger.Ledger
		err = db.NewSelect().
			Model(&ledger3).
			Where("name = ?", ledger3Name).
			Scan(ctx)
		require.NoError(t, err)
		require.NotNil(t, ledger3.DeletedAt, "ledger3 should have deleted_at set")
		require.Equal(t, bucket3Name, ledger3.Bucket)
	})
}

var (
	workerTestSrv *pgtesting.PostgresServer
)

func newStoreAndDBForWorkerTest(t docker.T) (*systemstore.DefaultStore, *bun.DB) {
	t.Helper()

	ctx := logging.TestingContext()
	pgDatabase := workerTestSrv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	debugHook := bundebug.NewQueryHook()
	debugHook.Debug = os.Getenv("DEBUG") == "true"
	hooks = append(hooks, debugHook)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), hooks...)
	require.NoError(t, err)

	store := systemstore.New(db)
	require.NoError(t, store.Migrate(ctx))

	return store, db
}

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		workerTestSrv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		return m.Run()
	})
}
