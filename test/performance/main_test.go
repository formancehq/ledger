//go:build it
// +build it

package performance_test

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/formancehq/ledger/test/performance/env"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"os"
	"testing"
)

var (
	authClientIDFlag     string
	authClientSecretFlag string
	scriptFlag           string

	// targeting a stack
	stackURLFlag string

	// targeting a ledger
	authIssuerURLFlag string
	ledgerURLFlag     string

	parallelismFlag int64
	reportFileFlag  string

	envFactory env.EnvFactory

	scripts = map[string]ActionProviderFactory{}
)

func init() {
	flag.StringVar(&stackURLFlag, "stack.url", "", "Stack URL")
	flag.StringVar(&authClientIDFlag, "client.id", "", "Client ID")
	flag.StringVar(&authClientSecretFlag, "client.secret", "", "Client secret")
	flag.StringVar(&ledgerURLFlag, "ledger.url", "", "Ledger url")
	flag.StringVar(&authIssuerURLFlag, "auth.url", "", "Auth url (ignored if --stack.url is specified)")
	flag.StringVar(&reportFileFlag, "report.file", "", "Location to write report file")
	flag.Int64Var(&parallelismFlag, "parallelism", 1, "Parallelism (default 1). Values is multiplied by GOMAXPROCS")
	flag.StringVar(&scriptFlag, "script", "", "Script to run")
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
	if authClientIDFlag != "" {
		httpClient = (&clientcredentials.Config{
			ClientID:     authClientIDFlag,
			ClientSecret: authClientSecretFlag,
			TokenURL:     authUrl + "/oauth/token",
			Scopes:       []string{"ledger:read", "ledger:write"},
		}).
			Client(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient))
	}

	return httpClient
}

func TestMain(m *testing.M) {
	if stackURLFlag != "" && ledgerURLFlag != "" {
		_, _ = fmt.Fprintf(os.Stderr, "Cannot specify both --stack.url and --ledger.url\n")
		os.Exit(1)
	}

	envFactory = env.DefaultEnvFactory

	switch {
	case stackURLFlag != "":
		envFactory = env.NewRemoteLedgerEnvFactory(getHttpClient(stackURLFlag+"/api/auth"), stackURLFlag+"/api/ledger")
	case ledgerURLFlag != "":
		envFactory = env.NewRemoteLedgerEnvFactory(getHttpClient(authIssuerURLFlag), ledgerURLFlag)
	}

	if envFactory == nil {
		_, _ = fmt.Fprintf(os.Stderr, "No env selected, you need to specify either --stack.url or --ledger.url\n")
		os.Exit(1)
	}

	os.Exit(m.Run())
}
