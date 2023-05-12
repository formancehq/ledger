package _17_optimized_segments

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/huandu/go-sqlbuilder"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestMigrate17(t *testing.T) {
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

	migrations, err := sqlstorage.CollectMigrationFiles(sqlstorage.MigrationsFS)
	require.NoError(t, err)

	modified, err := sqlstorage.Migrate(context.Background(), schema, migrations[0:17]...)
	require.NoError(t, err)
	require.True(t, modified)

	now := time.Now().UTC().Truncate(time.Second)

	ib := sqlbuilder.NewInsertBuilder()
	sqlq, args := ib.
		InsertInto(schema.Table("transactions")).
		Cols("id", "timestamp", "postings", "metadata").
		Values(0, now.Format(time.RFC3339), `[
			{"source": "world", "destination": "users:001", "asset": "USD", "amount": 100}
		]`, "{}").
		BuildWithFlavor(schema.Flavor())
	_, err = schema.ExecContext(context.Background(), sqlq, args...)
	require.NoError(t, err)

	modified, err = sqlstorage.Migrate(context.Background(), schema, migrations[17])
	require.NoError(t, err)
	require.True(t, modified)

	sqlq, args = sqlbuilder.
		Select("txid", "posting_index", "source", "destination").
		From(schema.Table("postings")).
		Where("txid = 0").
		BuildWithFlavor(schema.Flavor())

	row := store.Schema().QueryRowContext(context.Background(), sqlq, args...)
	require.NoError(t, row.Err())

	var txid uint64
	var postingIndex int
	var source, destination string
	require.NoError(t, err, row.Scan(&txid, &postingIndex, &source, &destination))
	require.Equal(t, uint64(0), txid)
	require.Equal(t, 0, postingIndex)
	require.Equal(t, `["world"]`, source)
	require.Equal(t, `["users", "001"]`, destination)
}
