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

	envFactory EnvFactory

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

//go:embed scripts
var scriptsDir embed.FS

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

func BenchmarkWrite(b *testing.B) {

	if stackURLFlag != "" && ledgerURLFlag != "" {
		b.Errorf("Cannot specify both --stack.url and --ledger.url")
		b.FailNow()
	}

	switch {
	case stackURLFlag != "":
		envFactory = NewRemoteLedgerEnvFactory(getHttpClient(stackURLFlag+"/api/auth"), stackURLFlag+"/api/ledger")
	case ledgerURLFlag != "":
		envFactory = NewRemoteLedgerEnvFactory(getHttpClient(authIssuerURLFlag), ledgerURLFlag)
	}

	// Load default scripts
	if scriptFlag == "" {
		entries, err := scriptsDir.ReadDir("scripts")
		require.NoError(b, err)

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			script, err := scriptsDir.ReadFile(filepath.Join("scripts", entry.Name()))
			require.NoError(b, err)

			scripts[strings.TrimSuffix(entry.Name(), ".js")] = NewJSActionProviderFactory(string(script))
		}
	} else {
		file, err := os.ReadFile(scriptFlag)
		require.NoError(b, err, "reading file "+scriptFlag)

		scripts["provided"] = NewJSActionProviderFactory(string(file))
	}

	if envFactory == nil {
		b.Errorf("no env selected, you need to specify either --stack.url or --ledger.url\n")
		b.FailNow()
	}

	// Execute benchmarks
	reports := New(b, envFactory, scripts).Run(logging.TestingContext())

	// Write report
	if reportFileFlag != "" {
		require.NoError(b, os.MkdirAll(filepath.Dir(reportFileFlag), 0755))

		f, err := os.Create(reportFileFlag)
		require.NoError(b, err)
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		require.NoError(b, enc.Encode(reports))
	}
}
