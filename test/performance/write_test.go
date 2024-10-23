//go:build it

package performance_test

import (
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"flag"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

	scripts = map[string]TransactionProviderFactory{}
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

//go:embed scripts
var scriptsDir embed.FS

// Init default scripts
func init() {
	entries, err := scriptsDir.ReadDir("scripts")
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		script, err := scriptsDir.ReadFile(filepath.Join("scripts", entry.Name()))
		if err != nil {
			panic(err)
		}

		scripts[strings.TrimSuffix(entry.Name(), ".js")] = NewJSTransactionProviderFactory(string(script))
	}
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


func BenchmarkWrite(b *testing.B) {

	if stackURL != "" && ledgerURL != "" {
		b.Errorf("Cannot specify both --stack.url and --ledger.url")
		b.FailNow()
	}

	switch {
	case stackURL != "":
		envFactory = NewRemoteLedgerEnvFactory(getHttpClient(stackURL+"/api/auth"), stackURL+"/api/ledger")
	case ledgerURL != "":
		envFactory = NewRemoteLedgerEnvFactory(getHttpClient(authIssuerURL), ledgerURL)
	}

	testing.Verbose()

	if envFactory == nil {
		b.Errorf("no env selected, you need to specify either --stack.url or --ledger.url\n")
		b.FailNow()
	}

	// Execute benchmarks
	reports := New(b, envFactory, scripts).Run(logging.TestingContext())

	// Write report
	if reportFile != "" {
		require.NoError(b, os.MkdirAll(filepath.Dir(reportFile), 0755))

		f, err := os.Create(reportFile)
		require.NoError(b, err)
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		require.NoError(b, enc.Encode(reports))
	}
}
