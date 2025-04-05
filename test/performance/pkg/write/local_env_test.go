//go:build it && local

package write

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/testing/docker"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/test/performance/pkg/env"

	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/pkg/testserver"
)

type TestServerEnv struct {
	testServer *testserver.Server
}

func (e *TestServerEnv) Client() *ledgerclient.SDK {
	return e.testServer.Client()
}

func (e *TestServerEnv) URL() string {
	return e.testServer.URL()
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
