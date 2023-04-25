package ledgerstore_test

import (
	"context"
	"os"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	_ "github.com/formancehq/ledger/pkg/storage/ledgerstore/migrates/0-init-schema"
	"github.com/formancehq/ledger/pkg/storage/schema"
	"github.com/formancehq/ledger/pkg/storage/utils"
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
	db, err := utils.OpenSQLDB(utils.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	driver := storage.NewDriver("postgres", schema.NewPostgresDB(db), ledgerstore.DefaultStoreConfig)
	require.NoError(t, driver.Initialize(context.Background()))
	ledgerStore, err := driver.CreateLedgerStore(context.Background(), uuid.NewString())
	require.NoError(t, err)

	_, err = ledgerStore.Migrate(context.Background())
	require.NoError(t, err)

	return ledgerStore
}

func appendLog(t *testing.T, store *ledgerstore.Store, log *core.Log) *core.PersistedLog {
	ret, err := store.AppendLog(context.Background(), core.NewActiveLog(log))
	<-ret.Done()
	require.NoError(t, err)
	return ret.Result()
}
