//go:build it && local

package write

import (
	"context"
	"flag"
	"io"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/bun/bunconnect"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v4/testing/deferred"
	"github.com/formancehq/go-libs/v4/testing/docker"
	"github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/testservice"
	"github.com/formancehq/go-libs/v4/time"

	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/test/performance/pkg/env"
)

func init() {
	flag.StringVar(&postgresURIFlag, "postgres-uri", "", "Postgres URI (optional - a containized version will be run if not specified - for local env only)")
}

var (
	postgresURIFlag string
)

type TestServerEnv struct {
	testServer *testservice.Service
}

func (e *TestServerEnv) Client() *ledgerclient.Formance {
	return testserver.Client(e.testServer)
}

func (e *TestServerEnv) URL() *url.URL {
	return testservice.GetServerURL(e.testServer)
}

func (e *TestServerEnv) Stop(ctx context.Context) error {
	return e.testServer.Stop(ctx)
}

var _ env.Env = (*TestServerEnv)(nil)

type TestServerEnvFactory struct {
	dockerPool *docker.Pool
}

func (f *TestServerEnvFactory) Create(ctx context.Context, b *testing.B) env.Env {

	f.dockerPool = docker.NewPool(b, logging.Testing())

	var connectionOptions bunconnect.ConnectionOptions
	if postgresURIFlag == "" {
		pgServer := pgtesting.CreatePostgresServer(b, f.dockerPool, pgtesting.WithPGCrypto())

		db := pgServer.NewDatabase(b)
		b.Logf("database: %s", db.Name())
		connectionOptions = db.ConnectionOptions()
	} else {
		connectionOptions.DatabaseSourceName = postgresURIFlag
	}

	connectionOptions.MaxOpenConns = 100
	connectionOptions.MaxIdleConns = 100
	connectionOptions.ConnMaxIdleTime = time.Minute

	var output io.Writer = os.Stdout
	if os.Getenv("DEBUG") != "true" {
		output = io.Discard
	}

	testServer := testserver.NewTestServer(
		deferred.FromValue(connectionOptions),
		testservice.WithInstruments(
			testservice.DebugInstrumentation(os.Getenv("DEBUG") == "true"),
			testservice.OutputInstrumentation(output),
			testservice.OTLPInstrumentation(deferred.FromValue(testservice.OTLPConfig{
				Metrics: &otlpmetrics.ModuleConfig{
					KeepInMemory:   true,
					RuntimeMetrics: true,
				},
			})),
			testserver.ExperimentalFeaturesInstrumentation(),
		),
	)
	require.NoError(b, testServer.Start(ctx))

	return &TestServerEnv{
		testServer: testServer,
	}
}

var _ env.EnvFactory = (*TestServerEnvFactory)(nil)

func NewTestServerEnvFactory() *TestServerEnvFactory {
	return &TestServerEnvFactory{}
}

func init() {
	env.FallbackEnvFactory = NewTestServerEnvFactory()
}
