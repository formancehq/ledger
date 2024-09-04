//go:build it

package performance_test

import (
	"context"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

type TestServerEnv struct {
	testServer *testserver.Server
	ledger     ledger.Ledger
}

func (e *TestServerEnv) Stop() error {
	e.testServer.Stop()
	return nil
}

func (e *TestServerEnv) Executor() TransactionExecutor {
	return TransactionExecutorFn(func(ctx context.Context, plain string, vars map[string]string) (*ledger.Transaction, error) {
		varsAsMapAny := make(map[string]any)
		for k, v := range vars {
			varsAsMapAny[k] = v
		}
		ret, err := e.testServer.Client().Ledger.V2.
			CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				Ledger: e.ledger.Name,
				V2PostTransaction: components.V2PostTransaction{
					Script: &components.V2PostTransactionScript{
						Plain: plain,
						Vars:  varsAsMapAny,
					},
				},
			})
		if err != nil {
			return nil, err
		}
		return &ledger.Transaction{
			ID: int(ret.V2CreateTransactionResponse.Data.ID.Int64()),
		}, nil
	})
}

var _ Env = (*TestServerEnv)(nil)

type TestServerEnvFactory struct {
	pgServer *pgtesting.PostgresServer
}

func (f *TestServerEnvFactory) Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env {

	db := f.pgServer.NewDatabase(b)
	b.Logf("database: %s", db.Name())
	connectionOptions := db.ConnectionOptions()
	connectionOptions.MaxOpenConns = 100
	connectionOptions.MaxIdleConns = 100
	connectionOptions.ConnMaxIdleTime = time.Minute

	var output io.Writer = os.Stdout
	if !testing.Verbose() {
		output = io.Discard
	}

	testServer := testserver.New(b, testserver.Configuration{
		PostgresConfiguration: connectionOptions,
		Debug:                 testing.Verbose(),
		Output:                output,
	})

	_, err := testServer.Client().Ledger.V2.
		CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: ledger.Name,
			V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
				Bucket:   pointer.For(ledger.Bucket),
				Metadata: ledger.Metadata,
				Features: ledger.Features,
			},
		})
	require.NoError(b, err)

	return &TestServerEnv{
		testServer: testServer,
		ledger:     ledger,
	}
}

var _ EnvFactory = (*TestServerEnvFactory)(nil)

func NewTestServerEnvFactory(pgServer *pgtesting.PostgresServer) *TestServerEnvFactory {
	return &TestServerEnvFactory{
		pgServer: pgServer,
	}
}
