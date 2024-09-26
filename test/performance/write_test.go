//go:build it

package performance_test

import (
	"fmt"
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
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

	// set default env factories if not defined (remote mode not used)
	if len(envFactories) == 0 {
		envFactories = map[string]EnvFactory{
			"core":       NewCoreEnvFactory(pgServer),
			"testserver": NewTestServerEnvFactory(pgServer),
		}
	}

	err := New(b, envFactories, scripts).Run(logging.TestingContext())
	require.NoError(b, err)
}
