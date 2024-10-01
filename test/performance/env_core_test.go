//go:build it

package performance_test

import (
	"context"
	systemstore "github.com/formancehq/ledger/internal/storage/driver"
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

type CoreEnv struct {
	writer     *ledgercontroller.DefaultController
	bunDB      *bun.DB
	pgDatabase *pgtesting.Database
}

func (e *CoreEnv) Stop(_ context.Context) error {
	return errors.Wrap(e.bunDB.Close(), "failed to close database connection")
}

func (e *CoreEnv) Executor() TransactionExecutor {
	return TransactionExecutorFn(func(ctx context.Context, plain string, vars map[string]string) (*ledger.Transaction, error) {
		ret, err := e.writer.CreateTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.RunScript]{
			Input: ledgercontroller.RunScript{
				Script: ledgercontroller.Script{
					Plain: plain,
					Vars:  vars,
				},
			},
		})
		if err != nil {
			return nil, err
		}

		return ret, nil
	})
}

var _ Env = (*CoreEnv)(nil)

type CoreEnvFactory struct {
	pgServer *pgtesting.PostgresServer
}

func (f *CoreEnvFactory) Create(ctx context.Context, b *testing.B, l ledger.Ledger) Env {
	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bundebug.NewQueryHook())
	}

	pgDatabase := f.pgServer.NewDatabase(b)
	b.Logf("database: %s", pgDatabase.Name())
	connectionOptions := pgDatabase.ConnectionOptions()
	connectionOptions.ConnMaxIdleTime = time.Minute
	connectionOptions.MaxOpenConns = 100
	connectionOptions.MaxIdleConns = 100

	bunDB, err := bunconnect.OpenSQLDB(ctx, connectionOptions, hooks...)
	require.NoError(b, err)
	require.NoError(b, systemstore.Migrate(ctx, bunDB))
	require.NoError(b, bucket.New(bunDB, l.Bucket).Migrate(ctx))
	require.NoError(b, ledgerstore.Migrate(ctx, bunDB, l))

	ledgerStore := ledgerstore.NewDefaultStoreAdapter(ledgerstore.New(bunDB, l))
	machineFactory := ledgercontroller.NewDefaultMachineFactory(
		ledgercontroller.NewCachedCompiler(
			ledgercontroller.NewDefaultCompiler(),
			ledgercontroller.CacheConfiguration{
				MaxCount: 10,
			},
		),
	)

	return &CoreEnv{
		writer:     ledgercontroller.NewDefaultController(l, ledgerStore, nil, machineFactory),
		bunDB:      bunDB,
		pgDatabase: pgDatabase,
	}
}

func NewCoreEnvFactory(pgServer *pgtesting.PostgresServer) *CoreEnvFactory {
	return &CoreEnvFactory{
		pgServer: pgServer,
	}
}

var _ EnvFactory = (*CoreEnvFactory)(nil)
