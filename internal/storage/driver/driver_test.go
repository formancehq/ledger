//go:build it

package driver_test

import (
	"fmt"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/google/uuid"
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/testing/docker"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func TestUpgradeAllBuckets(t *testing.T) {
	t.Parallel()

	d := newStorageDriver(t)
	ctx := logging.TestingContext()

	count := 30

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("ledger%d", i)
		_, err := d.CreateBucket(ctx, name)
		require.NoError(t, err)
	}

	require.NoError(t, d.UpgradeAllBuckets(ctx))
}

func TestLedgersCreate(t *testing.T) {
	ctx := logging.TestingContext()
	driver := newStorageDriver(t)

	l := ledger.MustNewWithDefault("foo")
	_, err := driver.CreateLedger(ctx, &l)
	require.NoError(t, err)
	require.Equal(t, 1, l.ID)
	require.NotEmpty(t, l.AddedAt)
}

func TestLedgersList(t *testing.T) {
	ctx := logging.TestingContext()
	driver := newStorageDriver(t)

	ledgers := make([]ledger.Ledger, 0)
	pageSize := uint64(2)
	count := uint64(10)
	for i := uint64(0); i < count; i++ {
		m := metadata.Metadata{}
		if i%2 == 0 {
			m["foo"] = "bar"
		}
		l := ledger.MustNewWithDefault(fmt.Sprintf("ledger%d", i)).WithMetadata(m)
		_, err := driver.CreateLedger(ctx, &l)
		require.NoError(t, err)

		ledgers = append(ledgers, l)
	}

	cursor, err := driver.ListLedgers(ctx, ledgercontroller.NewListLedgersQuery(pageSize))
	require.NoError(t, err)
	require.Len(t, cursor.Data, int(pageSize))
	require.Equal(t, ledgers[:pageSize], cursor.Data)

	for i := pageSize; i < count; i += pageSize {
		query := ledgercontroller.ListLedgersQuery{}
		require.NoError(t, bunpaginate.UnmarshalCursor(cursor.Next, &query))

		cursor, err = driver.ListLedgers(ctx, query)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		require.Equal(t, ledgers[i:i+pageSize], cursor.Data)
	}
}

func TestLedgerUpdateMetadata(t *testing.T) {
	ctx := logging.TestingContext()
	storageDriver := newStorageDriver(t)

	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := storageDriver.CreateLedger(ctx, &l)
	require.NoError(t, err)

	addedMetadata := metadata.Metadata{
		"foo": "bar",
	}
	err = storageDriver.UpdateLedgerMetadata(ctx, l.Name, addedMetadata)
	require.NoError(t, err)

	ledgerFromDB, err := storageDriver.GetLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, addedMetadata, ledgerFromDB.Metadata)
}

func TestLedgerDeleteMetadata(t *testing.T) {
	ctx := logging.TestingContext()
	driver := newStorageDriver(t)

	l := ledger.MustNewWithDefault(uuid.NewString()).WithMetadata(metadata.Metadata{
		"foo": "bar",
	})

	_, err := driver.CreateLedger(ctx, &l)
	require.NoError(t, err)

	err = driver.DeleteLedgerMetadata(ctx, l.Name, "foo")
	require.NoError(t, err)

	ledgerFromDB, err := driver.GetLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, metadata.Metadata{}, ledgerFromDB.Metadata)
}

func newStorageDriver(t docker.T) *driver.Driver {
	t.Helper()

	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bundebug.NewQueryHook())
	}
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), hooks...)
	require.NoError(t, err)

	d := driver.New(db)

	require.NoError(t, d.Initialize(logging.TestingContext()))

	return d
}
