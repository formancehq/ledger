//go:build it

package ledgerstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/formancehq/go-libs/testing/docker"
	"github.com/formancehq/go-libs/testing/utils"

	"github.com/formancehq/go-libs/bun/bunconnect"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	srv   *pgtesting.PostgresServer
	bunDB *bun.DB
)

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		db, err := sql.Open("postgres", srv.GetDSN())
		if err != nil {
			logging.Error(err)
			os.Exit(1)
		}

		bunDB = bun.NewDB(db, pgdialect.New())

		return m.Run()
	})
}

type T interface {
	require.TestingT
	Helper()
	Cleanup(func())
}

func newBucket(t T, hooks ...bun.QueryHook) *Bucket {
	name := uuid.NewString()
	ctx := logging.TestingContext()

	pgDatabase := srv.NewDatabase(t)

	connectionOptions := bunconnect.ConnectionOptions{
		DatabaseSourceName: pgDatabase.ConnString(),
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
