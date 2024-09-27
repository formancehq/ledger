package cmd

import (
	"io"
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func TestDocEvents(t *testing.T) {
	t.Parallel()

	cmd := NewDocEventsCommand()
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{
		"--write-dir", t.TempDir(),
	})
	require.NoError(t, cmd.ExecuteContext(logging.TestingContext()))
}
