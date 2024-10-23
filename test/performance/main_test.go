//go:build it

package performance_test

import (
	"context"
	"crypto/tls"
	"flag"
	. "github.com/formancehq/go-libs/v2/testing/utils"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"testing"
)

var (
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
		if stackURL != "" && ledgerURL != "" {
			t.Errorf("Cannot specify both --stack.url and --ledger.url")
			t.FailNow()
		}

		switch {
		case stackURL != "":
			envFactory = NewRemoteLedgerEnvFactory(getHttpClient(stackURL+"/api/auth"), stackURL+"/api/ledger")
		case ledgerURL != "":
			envFactory = NewRemoteLedgerEnvFactory(getHttpClient(authIssuerURL), ledgerURL)
		}

		testing.Verbose()

		if envFactory == nil {
			t.Errorf("no env selected, you need to specify either --stack.url or --ledger.url\n")
			t.FailNow()
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
