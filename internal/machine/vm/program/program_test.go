package program_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/machine/script/compiler"
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
