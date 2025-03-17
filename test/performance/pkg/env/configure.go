//go:build it

package env

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/formancehq/go-libs/v2/httpclient"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"os"
)

var (
	authClientIDFlag     string
	authClientSecretFlag string

	// targeting a stack
	stackURLFlag string

	// targeting a ledger
	authIssuerURLFlag string
	ledgerURLFlag     string

	Factory EnvFactory
)

func init() {
	flag.StringVar(&stackURLFlag, "stack.url", "", "Stack URL")
	flag.StringVar(&authClientIDFlag, "client.id", "", "Client ID")
	flag.StringVar(&authClientSecretFlag, "client.secret", "", "Client secret")
	flag.StringVar(&ledgerURLFlag, "ledger.url", "", "Ledger url")
	flag.StringVar(&authIssuerURLFlag, "auth.url", "", "Auth url (ignored if --stack.url is specified)")
}

func GetHTTPClient() *http.Client {

	authUrl := ""
	switch {
	case stackURLFlag != "":
		authUrl = stackURLFlag + "/api/auth"
	case ledgerURLFlag != "":
		authUrl = authIssuerURLFlag
	}

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
	if os.Getenv("DEBUG") == "true" {
		httpClient.Transport = httpclient.NewDebugHTTPTransport(httpClient.Transport)
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

func Start() {
	flag.Parse()

	if stackURLFlag != "" && ledgerURLFlag != "" {
		_, _ = fmt.Fprintf(os.Stderr, "Cannot specify both --stack.url and --ledger.url\n")
		os.Exit(1)
	}

	Factory = FallbackEnvFactory

	switch {
	case stackURLFlag != "":
		Factory = NewRemoteLedgerEnvFactory(GetHTTPClient(), stackURLFlag+"/api/ledger")
	case ledgerURLFlag != "":
		Factory = NewRemoteLedgerEnvFactory(GetHTTPClient(), ledgerURLFlag)
	}

	if Factory == nil {
		_, _ = fmt.Fprintf(os.Stderr, "No env selected, you need to specify either --stack.url or --ledger.url\n")
		os.Exit(1)
	}
}
