package clean_logs_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestMigrate(t *testing.T) {
	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		require.NoError(t, pgtesting.DestroyPostgresServer())
	}()

	driver, closeFunc, err := ledgertesting.StorageDriver(t)
	require.NoError(t, err)
	defer closeFunc()

	require.NoError(t, driver.Initialize(context.Background()))
	store, _, err := driver.GetLedgerStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	schema := store.Schema()

	ms, err := migrations.CollectMigrationFiles(ledgerstore.MigrationsFS)
	require.NoError(t, err)

	modified, err := migrations.Migrate(context.Background(), schema, ms[0:13]...)
	require.NoError(t, err)
	require.True(t, modified)

	ls := []ledgerstore.Log{
		{
			ID:   0,
			Type: core.NewTransactionType,
			Hash: "",
			Date: time.Now(),
			Data: []byte(`{
				"txid": 0,
				"postings": [],
				"reference": "tx1"
			}`),
		},
		{
			ID:   1,
			Type: core.NewTransactionType,
			Hash: "",
			Date: time.Now(),
			Data: []byte(`{
				"txid": 1,
				"postings": [],
				"preCommitVolumes": {},
				"postCommitVolumes": {},
				"reference": "tx2"
			}`),
		},
	}
	_, err = schema.NewInsert(ledgerstore.LogTableName).
		Model(&ls).
		Exec(context.Background())
	require.NoError(t, err)

	modified, err = migrations.Migrate(context.Background(), schema, ms[13])
	require.NoError(t, err)
	require.True(t, modified)

	sb := schema.NewSelect(ledgerstore.LogTableName).
		Model((*ledgerstore.Log)(nil)).
		Column("data")

	rows, err := schema.QueryContext(context.Background(), sb.String())
	require.NoError(t, err)

	require.True(t, rows.Next())
	var dataStr string
	require.NoError(t, rows.Scan(&dataStr))

	data := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(dataStr), &data))

	require.Equal(t, map[string]any{
		"txid":      float64(0),
		"postings":  []interface{}{},
		"reference": "tx1",
	}, data)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&dataStr))
	require.NoError(t, json.Unmarshal([]byte(dataStr), &data))

	require.Equal(t, map[string]any{
		"txid":      float64(1),
		"postings":  []interface{}{},
		"reference": "tx2",
	}, data)

}
