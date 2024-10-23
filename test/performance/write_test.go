//go:build it

package performance_test

import (
	"embed"
	"encoding/json"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var scripts = map[string]TransactionProviderFactory{}

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

func BenchmarkWrite(b *testing.B) {

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
