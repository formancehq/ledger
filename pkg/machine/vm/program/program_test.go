package program_test

import (
	"testing"

	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/stretchr/testify/require"
)

func TestProgram_String(t *testing.T) {
	p, err := compiler.Compile(`
		send [COIN 99] (
			source = @world
			destination = @alice
		)`)
	require.NoError(t, err)
	_ = p.String()
}
