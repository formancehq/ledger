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

func TestAddLearnerCommandRejectsMalformedAddresses(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{name: "raft", args: []string{"1", "not an address", "node-1:8888", "000102030405060708090a0b0c0d0e0f"}, want: "raft address \"not an address\" must be host:port"},
		{name: "service", args: []string{"1", "node-1:7777", "also bad", "000102030405060708090a0b0c0d0e0f"}, want: "service address \"also bad\" must be host:port"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Rejected before any client connection is configured or dialed.
			cmd := NewAddLearnerCommand()
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true
			cmd.SetArgs(tt.args)

			require.ErrorContains(t, cmd.Execute(), tt.want)
		})
	}
}
