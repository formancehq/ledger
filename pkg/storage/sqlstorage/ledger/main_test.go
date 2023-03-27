package ledger_test

import (
	"context"
	"os"
	"testing"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
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

func newLedgerStore(t *testing.T) storage.LedgerStore {
	t.Helper()

	pgServer := pgtesting.NewPostgresDatabase(t)
	db, err := sqlstorage.OpenSQLDB(pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	driver := sqlstorage.NewDriver("postgres", schema.NewPostgresDB(db))
	require.NoError(t, driver.Initialize(context.Background()))

	ledgerStore, _, err := driver.GetLedgerStore(context.Background(), uuid.NewString(), true)
	require.NoError(t, err)

	_, err = ledgerStore.Initialize(context.Background())
	require.NoError(t, err)

	return ledgerStore
}
