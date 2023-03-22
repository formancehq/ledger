package clean_logs_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

type Log struct {
	bun.BaseModel `bun:"log,alias:log"`

	ID   uint64          `bun:"id,unique,type:bigint"`
	Type string          `bun:"type,type:varchar"`
	Hash string          `bun:"hash,type:varchar"`
	Date core.Time       `bun:"date,type:timestamptz"`
	Data json.RawMessage `bun:"data,type:jsonb"`
}

func TestMigrate(t *testing.T) {
	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		require.NoError(t, pgtesting.DestroyPostgresServer())
	}()

	driver := ledgertesting.StorageDriver(t)

	require.NoError(t, driver.Initialize(context.Background()))
	store, _, err := driver.GetLedgerStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	schema := store.(*ledgerstore.Store).Schema()

	ms, err := migrations.CollectMigrationFiles(ledgerstore.MigrationsFS)
	require.NoError(t, err)

	modified, err := migrations.Migrate(context.Background(), schema, ms[0:13]...)
	require.NoError(t, err)
	require.True(t, modified)

	ls := []Log{
		{
			ID:   0,
			Type: core.NewTransactionLogType.String(),
			Hash: "",
			Date: core.Now(),
			Data: []byte(`{
				"txid": 0,
				"postings": [],
				"reference": "tx1"
			}`),
		},
		{
			ID:   1,
			Type: core.NewTransactionLogType.String(),
			Hash: "",
			Date: core.Now(),
			Data: []byte(`{
				"txid": 1,
				"postings": [],
				"preCommitVolumes": {},
				"postCommitVolumes": {},
				"reference": "tx2"
			}`),
		},
	}
	_, err = schema.NewInsert("log").
		Model(&ls).
		Exec(context.Background())
	require.NoError(t, err)

	modified, err = migrations.Migrate(context.Background(), schema, ms[13])
	require.NoError(t, err)
	require.True(t, modified)

	sb := schema.NewSelect("log").
		Model((*Log)(nil)).
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
