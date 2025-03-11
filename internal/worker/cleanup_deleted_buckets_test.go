package worker

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/docker"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	. "github.com/formancehq/go-libs/v2/testing/utils"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

var (
	srv *pgtesting.PostgresServer
	db  *bun.DB
)

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))
		sqlDB, err := sql.Open("pgx", srv.GetDSN())
		require.NoError(t, err)

		db = bun.NewDB(sqlDB, pgdialect.New())
		hook := bundebug.NewQueryHook()
		hook.Debug = false
		db.AddQueryHook(hook)

		return m.Run()
	})
}

func TestCleanupDeletedBuckets(t *testing.T) {
	t.Parallel()

	// Create a context with a reasonable timeout for the test
	ctx, cancel := context.WithTimeout(logging.TestingContext(), 5*time.Second)
	defer cancel()

	store := newTestStore(t)

	// Create test data
	bucket := "test_bucket"
	ledger1, err := ledger.New("ledger1", ledger.Configuration{
		Bucket: bucket,
	})
	require.NoError(t, err)
	ledger2, err := ledger.New("ledger2", ledger.Configuration{
		Bucket: bucket,
	})
	require.NoError(t, err)
	ledger3, err := ledger.New("ledger3", ledger.Configuration{
		Bucket: bucket,
	})
	require.NoError(t, err)

	// Create ledgers in the bucket
	err = store.CreateLedger(ctx, ledger1)
	require.NoError(t, err)
	err = store.CreateLedger(ctx, ledger2)
	require.NoError(t, err)
	err = store.CreateLedger(ctx, ledger3)
	require.NoError(t, err)

	// Mark bucket as deleted
	err = store.MarkBucketAsDeleted(ctx, bucket)
	require.NoError(t, err)

	// Create worker with immediate execution schedule
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse("*/1 * * * * *") // Run every second
	require.NoError(t, err)

	worker := NewCleanupDeletedBucketsRunner(logging.Testing(), db, store, CleanupDeletedBucketsConfig{
		RetentionDays: 0, // Set to 0 for immediate cleanup in test
		Schedule:      schedule,
	})

	// Run cleanup in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- worker.Run(ctx)
	}()

	// Wait for a short period to allow the worker to run at least once
	time.Sleep(2 * time.Second)

	// Stop the worker
	cancel()

	// Wait for the worker to stop and check for errors
	select {
	case err = <-errChan:
		if err != nil && err != context.Canceled {
			require.NoError(t, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop within timeout")
	}

	// Verify ledgers are physically deleted
	_, err = store.GetLedger(ctx, ledger1.Name)
	require.Error(t, err)
	_, err = store.GetLedger(ctx, ledger2.Name)
	require.Error(t, err)
	_, err = store.GetLedger(ctx, ledger3.Name)
	require.Error(t, err)
}

func newTestStore(t *testing.T) system.Store {
	t.Helper()

	database := srv.NewDatabase(t)
	bunDB, err := bunconnect.OpenSQLDB(context.Background(), database.ConnectionOptions())
	require.NoError(t, err)

	store := system.New(bunDB)
	require.NoError(t, store.Migrate(context.Background()))

	return store
}
