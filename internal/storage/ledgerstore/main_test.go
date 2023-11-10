package ledgerstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/formancehq/ledger/internal/storage/sqlutils"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

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

func newLedgerStore(t *testing.T) *Store {
	t.Helper()

	ledgerName := uuid.NewString()

	_, err := bunDB.Exec(fmt.Sprintf(`create schema if not exists "%s"`, ledgerName))
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err = bunDB.Exec(fmt.Sprintf(`drop schema "%s" cascade`, ledgerName))
		require.NoError(t, err)
	})

	ledgerDB, err := sqlutils.OpenDBWithSchema(sqlutils.ConnectionOptions{
		DatabaseSourceName: pgtesting.Server().GetDSN(),
		Debug:              testing.Verbose(),
	}, ledgerName)
	require.NoError(t, err)

	store, err := New(ledgerDB, ledgerName, func(ctx context.Context) error {
		return nil
	})
	require.NoError(t, err)

	_, err = store.Migrate(logging.TestingContext())
	require.NoError(t, err)

	return store
}

func appendLog(t *testing.T, store *Store, log *ledger.ChainedLog) *ledger.ChainedLog {
	err := store.InsertLogs(context.Background(), log)
	require.NoError(t, err)
	return log
}
