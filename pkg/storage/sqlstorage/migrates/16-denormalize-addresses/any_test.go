package _16_denormalize_addresses

import (
	"context"
	"testing"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestMigrate16(t *testing.T) {
	driver, closeFunc, err := ledgertesting.StorageDriver()
	require.NoError(t, err)
	defer closeFunc()

	require.NoError(t, driver.Initialize(context.Background()))
	store, _, err := driver.GetLedgerStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	schema := store.Schema()

	migrations, err := sqlstorage.CollectMigrationFiles(sqlstorage.MigrationsFS)
	require.NoError(t, err)

	modified, err := sqlstorage.Migrate(context.Background(), schema, migrations[0:16]...)
	require.NoError(t, err)
	require.True(t, modified)

	now := time.Now().UTC().Truncate(time.Second)

	ib := sqlbuilder.NewInsertBuilder()
	sqlq, args := ib.
		InsertInto(schema.Table("transactions")).
		Cols("id", "timestamp", "postings", "metadata").
		Values(0, now.Format(time.RFC3339), `[
			{"source": "world", "destination": "bank", "asset": "USD", "amount": 100},
			{"source": "bank", "destination": "user", "asset": "USD", "amount": 100}
		]`, "{}").
		BuildWithFlavor(schema.Flavor())
	_, err = schema.ExecContext(context.Background(), sqlq, args...)
	require.NoError(t, err)

	modified, err = sqlstorage.Migrate(context.Background(), schema, migrations[16])
	require.NoError(t, err)
	require.True(t, modified)

	sqlq, args = sqlbuilder.
		Select("sources", "destinations").
		From(schema.Table("transactions")).
		Where("id = 0").
		BuildWithFlavor(schema.Flavor())

	row := store.Schema().QueryRowContext(context.Background(), sqlq, args...)
	require.NoError(t, row.Err())
	var sources, destinations string
	require.NoError(t, err, row.Scan(&sources, &destinations))
	require.Equal(t, "world;bank", sources)
	require.Equal(t, "bank;user", destinations)
}
