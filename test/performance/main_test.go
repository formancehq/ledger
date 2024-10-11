//go:build it

package performance_test

import (
	"context"
	"crypto/tls"
	"flag"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/docker"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	. "github.com/formancehq/go-libs/testing/utils"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"testing"
)

var (
	dockerPool *docker.Pool
	pgServer   *Deferred[*pgtesting.PostgresServer]

	authClientID     string
	authClientSecret string

	// targeting a stack
	stackURL string

	// targeting a ledger
	authIssuerURL string
	ledgerURL     string

	parallelism int64
	reportFile  string

	envFactory EnvFactory
)

func init() {
	flag.StringVar(&stackURL, "stack.url", "", "Stack URL")
	flag.StringVar(&authClientID, "client.id", "", "Client ID")
	flag.StringVar(&authClientSecret, "client.secret", "", "Client secret")
	flag.StringVar(&ledgerURL, "ledger.url", "", "Ledger url")
	flag.StringVar(&authIssuerURL, "auth.url", "", "Auth url (ignored if --stack.url is specified)")
	flag.StringVar(&reportFile, "report.file", "", "Location to write report file")
	flag.Int64Var(&parallelism, "parallelism", 1, "Parallelism (default 1). Values is multiplied by GOMAXPROCS")
}

func TestMain(m *testing.M) {
	flag.Parse()

	WithTestMain(func(t *TestingTForMain) int {
		selectedEnv := 0
		if stackURL != "" {
			selectedEnv++
		}
		if ledgerURL != "" {
			selectedEnv++
		}
		if selectedEnv > 1 {
			t.Errorf("Cannot specify both --stack.url and --ledger.url")
			t.FailNow()
		}

		switch {
		case stackURL != "":
			envFactory = NewRemoteLedgerEnvFactory(getHttpClient(stackURL+"/api/auth"), stackURL+"/api/ledger")
		case ledgerURL != "":
			envFactory = NewRemoteLedgerEnvFactory(getHttpClient(authIssuerURL), ledgerURL)
		default:
			// Configure the environment to run benchmarks locally.
			// Start a docker connection and create a new postgres server.
			dockerPool = docker.NewPool(t, logging.Testing())

			pgServer = NewDeferred[*pgtesting.PostgresServer]()
			pgServer.LoadAsync(func() *pgtesting.PostgresServer {
				return pgtesting.CreatePostgresServer(
					t,
					dockerPool,
					pgtesting.WithPGCrypto(),
				)
			})

			Wait(pgServer)

			envFactory = NewTestServerEnvFactory(pgServer.GetValue())
		}

		return m.Run()
	})
}

func getHttpClient(authUrl string) *http.Client {
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxConnsPerHost:     100,
			MaxIdleConnsPerHost: 100,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	if authClientID != "" {
		httpClient = (&clientcredentials.Config{
			ClientID:     authClientID,
			ClientSecret: authClientSecret,
			TokenURL:     authUrl + "/oauth/token",
			Scopes:       []string{"ledger:read", "ledger:write"},
		}).
			Client(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient))
	}

	return httpClient
}
