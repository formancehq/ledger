//go:build it

package performance_test

import (
	"context"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	ledgerclient "github.com/formancehq/stack/ledger/client"
	"io"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"github.com/stretchr/testify/require"
)

type TestServerEnv struct {
	testServer *testserver.Server
	ledger     ledger.Ledger
}

func (e *TestServerEnv) Client() *ledgerclient.Formance {
	return e.testServer.Client()
}

func (e *TestServerEnv) URL() string {
	return e.testServer.URL()
}

func (e *TestServerEnv) Stop(ctx context.Context) error {
	e.testServer.Stop(ctx)
	return nil
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
		OTLPConfig: &testserver.OTLPConfig{
			Metrics: &otlpmetrics.ModuleConfig{
				KeepInMemory:   true,
				RuntimeMetrics: true,
			},
		},
		ExperimentalFeatures: true,
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
