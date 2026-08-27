package cluster

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddLearnerCommandRequiresInstanceID(t *testing.T) {
	t.Parallel()

	cmd := NewAddLearnerCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"1", "node-1:7777", "node-1:8888"})

	require.ErrorContains(t, cmd.Execute(), "accepts 4 arg")
}

func TestAddLearnerCommandRejectsInvalidInstanceID(t *testing.T) {
	t.Parallel()

	t.Run("not hexadecimal", func(t *testing.T) {
		t.Parallel()

		cmd := NewAddLearnerCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		cmd.SetArgs([]string{"1", "node-1:7777", "node-1:8888", "not-hex"})

		require.ErrorContains(t, cmd.Execute(), "expected hexadecimal")
	})

	t.Run("wrong decoded length", func(t *testing.T) {
		t.Parallel()

		cmd := NewAddLearnerCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		cmd.SetArgs([]string{"1", "node-1:7777", "node-1:8888", "0123"})

		require.ErrorContains(t, cmd.Execute(), "instance_id must be 16 bytes")
	})
}
