package clean_logs_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestMigrate(t *testing.T) {
	driver, closeFunc, err := ledgertesting.StorageDriver()
	require.NoError(t, err)
	defer closeFunc()

	require.NoError(t, driver.Initialize(context.Background()))
	store, _, err := driver.GetLedgerStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	schema := store.Schema()

	migrations, err := sqlstorage.CollectMigrationFiles(sqlstorage.MigrationsFS)
	require.NoError(t, err)

	modified, err := sqlstorage.Migrate(context.Background(), schema, migrations[0:13]...)
	require.NoError(t, err)
	require.True(t, modified)

	sqlq, args := sqlbuilder.NewInsertBuilder().
		InsertInto(schema.Table("log")).
		Cols("id", "type", "hash", "date", "data").
		Values("0", core.NewTransactionType, "", time.Now(), `{
			"txid": 0,
			"postings": [],
			"reference": "tx1"
		}`).
		Values("1", core.NewTransactionType, "", time.Now(), `{
			"txid": 1,
			"postings": [],
			"preCommitVolumes": {},
			"postCommitVolumes": {},
			"reference": "tx2"
		}`).
		BuildWithFlavor(schema.Flavor())

	_, err = schema.ExecContext(context.Background(), sqlq, args...)
	require.NoError(t, err)

	modified, err = sqlstorage.Migrate(context.Background(), schema, migrations[13])
	require.NoError(t, err)
	require.True(t, modified)

	sqlq, args = sqlbuilder.NewSelectBuilder().
		Select("data").
		From(schema.Table("log")).
		BuildWithFlavor(schema.Flavor())

	rows, err := schema.QueryContext(context.Background(), sqlq, args...)
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
