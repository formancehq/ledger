package ledgertesting

import (
	"context"
	"os"

	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/api/idempotency"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"go.uber.org/fx"
)

func StorageDriverName() string {
	fromEnv := os.Getenv("NUMARY_STORAGE_DRIVER")
	if fromEnv != "" {
		return fromEnv
	}
	return "sqlite"
}

func StorageDriver(multipleInstance bool) (*sqlstorage.Driver, func(), error) {
	switch StorageDriverName() {
	case "sqlite":
		id := uuid.New()
		return sqlstorage.NewDriver("sqlite", sqlstorage.NewSQLiteDB(os.TempDir(), id), multipleInstance), func() {}, nil
	case "postgres":
		pgServer, err := pgtesting.PostgresServer()
		if err != nil {
			return nil, nil, err
		}
		db, err := sqlstorage.OpenSQLDB(sqlstorage.PostgreSQL, pgServer.ConnString())
		if err != nil {
			return nil, nil, err
		}
		return sqlstorage.NewDriver(
				"postgres",
				sqlstorage.NewPostgresDB(db),
				multipleInstance,
			), func() {
				_ = pgServer.Close()
			}, nil
	}
	return nil, nil, errors.New("not found driver")
}

func ProvideStorageDriver(withMultipleInstance bool) fx.Option {
	return fx.Provide(func(lc fx.Lifecycle) (*sqlstorage.Driver, error) {
		driver, stopFn, err := StorageDriver(withMultipleInstance)
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{
			OnStart: driver.Initialize,
			OnStop: func(ctx context.Context) error {
				stopFn()
				return driver.Close(ctx)
			},
		})
		return driver, nil
	})
}

func ProvideLedgerStorageDriver() fx.Option {
	return fx.Options(
		ProvideStorageDriver(false),
		fx.Provide(
			fx.Annotate(sqlstorage.NewLedgerStorageDriverFromRawDriver,
				fx.As(new(storage.Driver[ledger.Store]))),
			fx.Annotate(sqlstorage.NewIdempotencyStorageDriverFromRawDriver,
				fx.As(new(storage.Driver[idempotency.Store]))),
		),
	)
}
