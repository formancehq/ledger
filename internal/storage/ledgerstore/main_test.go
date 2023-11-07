package ledgerstore_test

import (
	"context"
	"os"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if err := pgtesting.CreatePostgresServer(); err != nil {
		logging.Error(err)
		os.Exit(1)
	}

	code := m.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		logging.Error(err)
	}
	os.Exit(code)
}

func newLedgerStore(t *testing.T) *ledgerstore.Store {
	t.Helper()

	pgServer := pgtesting.NewPostgresDatabase(t)
	db, err := storage.OpenSQLDB(storage.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
	},
	//&explainHook{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := logging.TestingContext()
	driver := driver.New(db)
	require.NoError(t, driver.Initialize(ctx))
	ledgerStore, err := driver.CreateLedgerStore(ctx, uuid.NewString())
	require.NoError(t, err)

	return ledgerStore
}

func appendLog(t *testing.T, store *ledgerstore.Store, log *ledger.ChainedLog) *ledger.ChainedLog {
	err := store.InsertLogs(context.Background(), log)
	require.NoError(t, err)
	return log
}
