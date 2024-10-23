//go:build it

package performance_test

import (
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

var scripts = map[string]TransactionProviderFactory{
	"world->bank":         TransparentTransactionProviderFactory(worldToBank),
	"world->any":          TransparentTransactionProviderFactory(worldToAny),
	"any(unbounded)->any": TransparentTransactionProviderFactory(anyUnboundedToAny),
	"any(bounded)->any":   TransparentTransactionProviderFactory(anyBoundedToAny),
}

func worldToBank(_ int) (string, map[string]string) {
	return `
send [USD/2 100] (
	source = @world
	destination = @bank
)`, nil
}

func worldToAny(id int) (string, map[string]string) {
	return `
vars {
	account $destination
}
send [USD/2 100] (
	source = @world
	destination = $destination
)`, map[string]string{
			"destination": fmt.Sprintf("dst:%d", id),
		}
}

func anyUnboundedToAny(id int) (string, map[string]string) {
	return `
vars {
	account $source
	account $destination
}
send [USD/2 100] (
	source = $source allowing unbounded overdraft
	destination = $destination
)`, map[string]string{
			"source":      fmt.Sprintf("src:%d", id),
			"destination": fmt.Sprintf("dst:%d", id),
		}
}

func anyBoundedToAny(id int) (string, map[string]string) {
	return fmt.Sprintf(`
vars {
	account $source
	account $destination
}
send [USD/2 100] (
	source = $source allowing overdraft up to [USD/2 %d]
	destination = $destination
)`, (id+1)*100), map[string]string{
			"source":      fmt.Sprintf("src:%d", id),
			"destination": fmt.Sprintf("dst:%d", id),
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
