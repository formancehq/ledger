//go:build it && local

package env

import (
	"context"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/testing/docker"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"io"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/testserver"
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
	return e.testServer.Stop(ctx)
}

var _ Env = (*TestServerEnv)(nil)

type TestServerEnvFactory struct {
	dockerPool *docker.Pool
}

func (f *TestServerEnvFactory) Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env {

	f.dockerPool = docker.NewPool(b, logging.Testing())

	pgServer := pgtesting.CreatePostgresServer(b, f.dockerPool, pgtesting.WithPGCrypto())

	db := pgServer.NewDatabase(b)
	b.Logf("database: %s", db.Name())
	connectionOptions := db.ConnectionOptions()
	connectionOptions.MaxOpenConns = 100
	connectionOptions.MaxIdleConns = 100
	connectionOptions.ConnMaxIdleTime = time.Minute

	var output io.Writer = os.Stdout
	if os.Getenv("DEBUG") != "true" {
		output = io.Discard
	}

	testServer := testserver.New(b, testserver.Configuration{
		CommonConfiguration: testserver.CommonConfiguration{
			PostgresConfiguration: connectionOptions,
			Debug:                 os.Getenv("DEBUG") == "true",
			Output:                output,
			OTLPConfig: &testserver.OTLPConfig{
				Metrics: &otlpmetrics.ModuleConfig{
					KeepInMemory:   true,
					RuntimeMetrics: true,
				},
			},
		},
		ExperimentalFeatures: true,
	})

	_, err := testServer.Client().Ledger.V2.
		CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: ledger.Name,
			V2CreateLedgerRequest: components.V2CreateLedgerRequest{
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

func NewTestServerEnvFactory() *TestServerEnvFactory {
	return &TestServerEnvFactory{}
}

func init() {
	DefaultEnvFactory = NewTestServerEnvFactory()
}
