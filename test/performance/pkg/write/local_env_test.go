//go:build it && local

package write

import (
	"context"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	. "github.com/formancehq/go-libs/v3/testing/utils"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/test/performance/pkg/env"
	"io"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/pkg/testserver"
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

	testServer := testserver.NewTestServer(
		NewValuedDeferred(connectionOptions),
		testservice.WithInstruments(
			testservice.DebugInstrumentation(os.Getenv("DEBUG") == "true"),
			testservice.OutputInstrumentation(output),
			testservice.OTLPInstrumentation(NewValuedDeferred(testservice.OTLPConfig{
				Metrics: &otlpmetrics.ModuleConfig{
					KeepInMemory:   true,
					RuntimeMetrics: true,
				},
			})),
			testserver.ExperimentalFeaturesInstrumentation(),
		),
	)

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
