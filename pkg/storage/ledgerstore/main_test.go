package ledgerstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/driver"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	_ "github.com/formancehq/ledger/pkg/storage/ledgerstore/migrates/0-init-schema"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
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
		Trace:              testing.Verbose(),
	},
	//&explainHook{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	driver := driver.New("postgres", storage.NewDatabase(db))
	require.NoError(t, driver.Initialize(context.Background()))
	ledgerStore, err := driver.CreateLedgerStore(context.Background(), uuid.NewString())
	require.NoError(t, err)

	_, err = ledgerStore.Migrate(context.Background())
	require.NoError(t, err)

	return ledgerStore
}

func appendLog(t *testing.T, store *ledgerstore.Store, log *core.ChainedLog) *core.ChainedLog {
	err := store.InsertLogs(context.Background(), core.NewActiveLog(log))
	require.NoError(t, err)
	return log
}

type explainHook struct{}

func (h *explainHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {}

func (h *explainHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	lowerQuery := strings.ToLower(event.Query)
	if strings.HasPrefix(lowerQuery, "explain") ||
		strings.HasPrefix(lowerQuery, "create") ||
		strings.HasPrefix(lowerQuery, "begin") ||
		strings.HasPrefix(lowerQuery, "alter") ||
		strings.HasPrefix(lowerQuery, "rollback") ||
		strings.HasPrefix(lowerQuery, "commit") {
		return ctx
	}

	event.DB.RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		rows, err := tx.Query("explain analyze " + event.Query)
		if err != nil {
			return err
		}
		defer rows.Next()

		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				return err
			}
			fmt.Println(line)
		}

		return tx.Rollback()

	})

	return ctx
}
