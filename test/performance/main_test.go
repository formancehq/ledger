//go:build it

package performance_test

import (
	"context"
	"crypto/tls"
	"flag"
	"net/http"
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/docker"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/testing/utils"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

var (
	dockerPool *docker.Pool
	pgServer   *pgtesting.PostgresServer

	authClientID     string
	authClientSecret string

	// targeting a stack
	stackURL string

	// targeting a ledger
	authIssuerURL string
	ledgerURL     string

	parallelism int64
	reportDir   string
	testCore bool

	envFactories = make(map[string]EnvFactory)
)

func init() {
	flag.StringVar(&stackURL, "stack.url", "", "Stack URL")
	flag.StringVar(&authClientID, "client.id", "", "Client ID")
	flag.StringVar(&authClientSecret, "client.secret", "", "Client secret")
	flag.StringVar(&ledgerURL, "ledger.url", "", "Ledger url")
	flag.StringVar(&authIssuerURL, "auth.url", "", "Auth url (ignored if --stack.url is specified)")
	flag.StringVar(&reportDir, "report.dir", "", "Location to write report files")
	flag.Int64Var(&parallelism, "parallelism", 1, "Parallelism (default 1). Values is multiplied by GOMAXPROCS")
	flag.BoolVar(&testCore, "test-core", false, "Test core only")
}

func TestMain(m *testing.M) {
	flag.Parse()

	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		selectedEnv := 0
		if stackURL != "" {
			selectedEnv++
		}
		if ledgerURL != "" {
			selectedEnv++
		}
		if testCore {
			selectedEnv++
		}
		if selectedEnv > 1 {
			t.Errorf("Cannot specify both --stack.url, --ledger.url or --test-core")
			t.FailNow()
		}

		switch {
		case stackURL != "":
			setupRemoteStackEnv()
		case ledgerURL != "":
			setRemoteLedgerEnv()
		case testCore:
			setCoreEnv()
		default:
			setupLocalEnv(t)
		}

		return m.Run()
	})
}

// setupLocalEnv configure the environment for running benchmarks locally
// is it start a docker connection and create a new postgres server
func setupLocalEnv(t *utils.TestingTForMain) {
	dockerPool = docker.NewPool(t, logging.Testing())
	pgServer = pgtesting.CreatePostgresServer(
		t,
		dockerPool,
		pgtesting.WithPGCrypto(),
	)
	envFactories = map[string]EnvFactory{
		"testserver": NewTestServerEnvFactory(pgServer),
	}
}

// setupRemoveEnv configure a remote env
func setupRemoteStackEnv() {
	envFactories["remote"] = NewRemoteStackEnvFactory(getHttpClient(stackURL+"/api/auth"), stackURL)
}

// setupRemoveEnv configure a remote env
func setRemoteLedgerEnv() {
	envFactories["remote"] = NewRemoteLedgerEnvFactory(getHttpClient(authIssuerURL), ledgerURL)
}

func setCoreEnv() {
	envFactories["core"] = NewCoreEnvFactory(pgServer)
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
