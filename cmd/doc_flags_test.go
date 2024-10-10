package cmd

import (
	"io"
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func TestDocFlags(t *testing.T) {
	t.Parallel()

	cmd := NewDocFlagsCommand()
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.ExecuteContext(logging.TestingContext()))
}
