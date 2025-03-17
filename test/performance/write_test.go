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

//go:embed scripts
var scriptsDir embed.FS

func BenchmarkWrite(b *testing.B) {

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

			rootPath, err := filepath.Abs("scripts")
			require.NoError(b, err)

			scripts[strings.TrimSuffix(entry.Name(), ".js")] = NewJSActionProviderFactory(rootPath, string(script))
		}
	} else {
		file, err := os.ReadFile(scriptFlag)
		require.NoError(b, err, "reading file "+scriptFlag)

		rootPath, err := filepath.Abs(filepath.Dir(scriptFlag))
		require.NoError(b, err)

		scripts["provided"] = NewJSActionProviderFactory(rootPath, string(file))
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
