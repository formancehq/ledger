package ledgerstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	bunDB *bun.DB
)

func TestMain(m *testing.M) {
	if err := pgtesting.CreatePostgresServer(); err != nil {
		logging.Error(err)
		os.Exit(1)
	}

	db, err := sql.Open("postgres", pgtesting.Server().GetDSN())
	if err != nil {
		logging.Error(err)
		os.Exit(1)
	}

	bunDB = bun.NewDB(db, pgdialect.New())

	code := m.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		logging.Error(err)
	}
	os.Exit(code)
}

type T interface {
	require.TestingT
	Helper()
	Cleanup(func())
}

func newBucket(t T, hooks ...bun.QueryHook) *Bucket {
	name := uuid.NewString()
	ctx := logging.TestingContext()

	pgDatabase := pgtesting.NewPostgresDatabase(t)

	connectionOptions := bunconnect.ConnectionOptions{
		DatabaseSourceName: pgDatabase.ConnString(),
		Debug:              testing.Verbose(),
		MaxIdleConns:       40,
		MaxOpenConns:       40,
		ConnMaxIdleTime:    time.Minute,
	}

	bucket, err := ConnectToBucket(ctx, connectionOptions, name, hooks...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = bucket.Close()
	})

	require.NoError(t, bucket.Migrate(ctx))

	return bucket
}

func newLedgerStore(t T, hooks ...bun.QueryHook) *Store {
	t.Helper()

	ledgerName := uuid.NewString()
	ctx := logging.TestingContext()

	_, err := bunDB.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, ledgerName))
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err = bunDB.ExecContext(ctx, fmt.Sprintf(`drop schema "%s" cascade`, ledgerName))
		require.NoError(t, err)
	})

	bucket := newBucket(t, hooks...)

	store, err := bucket.CreateLedgerStore(ledgerName)
	require.NoError(t, err)

	return store
}

func appendLog(t *testing.T, store *Store, log *ledger.ChainedLog) *ledger.ChainedLog {
	err := store.InsertLogs(context.Background(), log)
	require.NoError(t, err)
	return log
}
