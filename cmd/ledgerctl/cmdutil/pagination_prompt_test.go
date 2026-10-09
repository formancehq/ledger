package cmdutil_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

func TestConfirmNextPageRejectsNonTerminalInput(t *testing.T) {
	t.Parallel()
	cmd := &cobra.Command{}
	input := strings.NewReader("q")
	var output, diagnostics bytes.Buffer
	cmd.SetIn(input)
	cmd.SetOut(&output)
	cmd.SetErr(&diagnostics)
	next, err := cmdutil.ConfirmNextPage(cmd)
	require.False(t, next)
	require.ErrorContains(t, err, "interactive pagination requires a terminal")
	require.Equal(t, 1, input.Len(), "non-terminal input must remain unread")
	require.Empty(t, output.String())
	require.Empty(t, diagnostics.String())
}

func TestConfirmNextPageAlreadyCanceledDoesNotPrompt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	var output, diagnostics bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&diagnostics)
	next, err := cmdutil.ConfirmNextPage(cmd)
	require.False(t, next)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, output.String())
	require.Empty(t, diagnostics.String())
}

func TestConfirmNextPageRejectsPipeWithoutConsumingInput(t *testing.T) {
	t.Parallel()
	input, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close(); _ = writer.Close() })
	_, err = writer.WriteString("q")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	cmd := &cobra.Command{}
	cmd.SetIn(input)
	var output, diagnostics bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&diagnostics)
	next, err := cmdutil.ConfirmNextPage(cmd)
	require.False(t, next)
	require.ErrorContains(t, err, "interactive pagination requires a terminal")
	var answer [1]byte
	n, err := input.Read(answer[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, byte('q'), answer[0])
	require.Empty(t, output.String())
	require.Empty(t, diagnostics.String())
}
