package ledgertesting

import (
	"context"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func StorageDriver(t pgtesting.TestingT) *sqlstorage.Driver {
	pgServer := pgtesting.NewPostgresDatabase(t)

	db, err := sqlstorage.OpenSQLDB(pgServer.ConnString())
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	return sqlstorage.NewDriver("postgres", schema.NewPostgresDB(db))
}

func ProvideStorageDriver(t pgtesting.TestingT) fx.Option {
	return fx.Provide(func(lc fx.Lifecycle) (storage.Driver, error) {
		driver := StorageDriver(t)
		lc.Append(fx.Hook{
			OnStart: driver.Initialize,
			OnStop: func(ctx context.Context) error {
				return driver.Close(ctx)
			},
		})
		return driver, nil
	})
}

func ProvideLedgerStorageDriver(t pgtesting.TestingT) fx.Option {
	return fx.Options(
		ProvideStorageDriver(t),
	)
}
