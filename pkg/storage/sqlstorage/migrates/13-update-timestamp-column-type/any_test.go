package _13_update_timestamp_column_type

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

func TestMigrate13(t *testing.T) {
	driver, closeFunc, err := ledgertesting.StorageDriver()
	require.NoError(t, err)
	defer closeFunc()

	require.NoError(t, driver.Initialize(context.Background()))
	store, _, err := driver.GetStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	schema := store.(*sqlstorage.Store).Schema()

	migrations, err := sqlstorage.CollectMigrationFiles(sqlstorage.MigrationsFS)
	require.NoError(t, err)

	modified, err := sqlstorage.Migrate(context.Background(), schema, migrations[0:13]...)
	require.NoError(t, err)
	require.True(t, modified)

	now := time.Now().UTC().Truncate(time.Second)

	ib := sqlbuilder.NewInsertBuilder()
	sqlq, args := ib.
		InsertInto(schema.Table("transactions")).
		Cols("id", "timestamp", "postings", "metadata").
		Values(0, now.Format(time.RFC3339), `[{"source": "world", "destination": "bank", "asset": "USD", "amount": 100}]`, "{}").
		BuildWithFlavor(schema.Flavor())
	_, err = schema.ExecContext(context.Background(), sqlq, args...)
	require.NoError(t, err)

	modified, err = sqlstorage.Migrate(context.Background(), schema, migrations[13])
	require.NoError(t, err)
	require.True(t, modified)

	tx, err := store.GetTransaction(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, now, tx.Timestamp)
	require.Len(t, tx.Postings, 1)
}
