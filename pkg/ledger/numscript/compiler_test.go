package numscript

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompiler(t *testing.T) {

	script := `send [USD/2 100] (
	source = @world
	destination = @bank
)`

	compiler := NewCompiler()
	p1, err := compiler.Compile(context.Background(), script)
	require.NoError(t, err)

	p2, err := compiler.Compile(context.Background(), script)
	require.NoError(t, err)

	require.Equal(t, p1, p2)
}
