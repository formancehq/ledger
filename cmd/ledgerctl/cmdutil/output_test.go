package cmdutil_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
)

func TestEncodeStructured(t *testing.T) {
	t.Parallel()

	type sample struct {
		Name  string `json:"name"  yaml:"name"`
		Count int    `json:"count" yaml:"count"`
	}

	data := sample{Name: "test", Count: 42}

	t.Run("json", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("json", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, data)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.Contains(t, out, `"name": "test"`)
		require.Contains(t, out, `"count": 42`)
	})

	t.Run("yaml", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("yaml", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, data)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.Contains(t, out, "name: test")
		require.Contains(t, out, "count: 42")
	})

	t.Run("no flag", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)

		handled, err := cmdutil.EncodeStructured(cmd, data)
		require.NoError(t, err)
		require.False(t, handled)
	})
}

func TestIsStructuredOutput(t *testing.T) {
	t.Parallel()

	t.Run("json", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("json", "true"))
		require.True(t, cmdutil.IsStructuredOutput(cmd))
	})

	t.Run("yaml", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("yaml", "true"))
		require.True(t, cmdutil.IsStructuredOutput(cmd))
	})

	t.Run("none", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.False(t, cmdutil.IsStructuredOutput(cmd))
	})
}

// captureStdout redirects os.Stdout to a pipe, calls fn, and returns the
// captured output as a string.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	old := os.Stdout
	os.Stdout = w

	defer func() { os.Stdout = old }()

	fn()

	require.NoError(t, w.Close())

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)

	return buf.String()
}
