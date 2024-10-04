//go:build it

package performance_test

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"

	"github.com/formancehq/go-libs/logging"
)

var scripts = map[string]func(int) (string, map[string]string){
	"world->bank":         worldToBank,
	"world->any":          worldToAny,
	"any(unbounded)->any": anyUnboundedToAny,
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

func BenchmarkWrite(b *testing.B) {

	// Set default env factories if not defined (remote mode not used)
	if len(envFactories) == 0 {
		envFactories = map[string]EnvFactory{
			"core":       NewCoreEnvFactory(pgServer),
			"testserver": NewTestServerEnvFactory(pgServer),
		}
	}

	// Execute benchmarks
	reports := New(b, envFactories, scripts).Run(logging.TestingContext())

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
