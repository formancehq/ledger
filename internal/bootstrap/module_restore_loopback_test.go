package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsLoopbackHost(t *testing.T) {
	t.Parallel()
	for host, want := range map[string]bool{
		"127.0.0.1": true,
		"127.0.0.2": true,
		"::1":       true,
		"localhost": true,
		"0.0.0.0":   false,
		"::":        false,
		"10.0.0.5":  false,
		"ledger-0":  false,
		"":          false,
	} {
		require.Equal(t, want, isLoopbackHost(host), host)
	}
}
