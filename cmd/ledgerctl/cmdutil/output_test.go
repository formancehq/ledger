package cmdutil_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestEncodeStructured is sequential because captureStdout mutates os.Stdout.
func TestEncodeStructured(t *testing.T) {
	type sample struct {
		Name  string `json:"name"  yaml:"name"`
		Count int    `json:"count" yaml:"count"`
	}

	data := sample{Name: "test", Count: 42}

	t.Run("json", func(t *testing.T) {
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
		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)

		handled, err := cmdutil.EncodeStructured(cmd, data)
		require.NoError(t, err)
		require.False(t, handled)
	})

	t.Run("json with --result-file mirrors payload to the file", func(t *testing.T) {
		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		cmd.Flags().String("result-file", "", "")
		require.NoError(t, cmd.Flags().Set("json", "true"))

		path := filepath.Join(t.TempDir(), "result.json")
		// writeResultFile opens with O_TRUNC (no O_CREATE) — matches the
		// kubelet's behaviour of pre-creating /dev/termination-log — so
		// pre-create the test target.
		require.NoError(t, os.WriteFile(path, []byte{}, 0o600))
		require.NoError(t, cmd.Flags().Set("result-file", path))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, data)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.Contains(t, out, `"name": "test"`)
		fileContent, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Contains(t, string(fileContent), `"name": "test"`)
		require.Contains(t, string(fileContent), `"count": 42`)
	})

	t.Run("json with --result-file pointing at unwritable path errors", func(t *testing.T) {
		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		cmd.Flags().String("result-file", "", "")
		require.NoError(t, cmd.Flags().Set("json", "true"))
		require.NoError(t, cmd.Flags().Set("result-file", "/nonexistent/dir/result.json"))

		_ = captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, data)
			require.True(t, handled)
			require.Error(t, err, "writing to a missing path must surface as an error so the caller doesn't think it succeeded")
		})
	})

	t.Run("proto message json uses camelCase", func(t *testing.T) {
		msg := &commonpb.NumscriptInfo{
			Name:      "myscript",
			Version:   "v1",
			CreatedAt: &commonpb.Timestamp{Data: 1000},
		}

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("json", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, msg)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.Contains(t, out, `"createdAt"`)
		require.NotContains(t, out, `"created_at"`)
		require.Contains(t, out, `"name"`)
		require.Contains(t, out, `"myscript"`)
	})

	t.Run("proto message yaml uses camelCase", func(t *testing.T) {
		msg := &commonpb.NumscriptInfo{
			Name:      "myscript",
			Version:   "v1",
			CreatedAt: &commonpb.Timestamp{Data: 1000},
		}

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("yaml", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, msg)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.Contains(t, out, "createdAt:")
		require.NotContains(t, out, "created_at:")
	})

	t.Run("proto slice json uses camelCase", func(t *testing.T) {
		msgs := []*commonpb.NumscriptInfo{
			{Name: "a", Version: "v1", CreatedAt: &commonpb.Timestamp{Data: 1000}},
			{Name: "b", Version: "v2"},
		}

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("json", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, msgs)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.Contains(t, out, `"createdAt"`)
		require.NotContains(t, out, `"created_at"`)
	})

	t.Run("transaction json renders uint256 as number and timestamp as ISO string", func(t *testing.T) {
		tx := &commonpb.Transaction{
			Id: 42,
			Postings: []*commonpb.Posting{
				{
					Source:      "world",
					Destination: "users:001",
					Amount:      commonpb.NewUint256FromUint64(5_000_000_000),
					Asset:       "USD",
				},
			},
			Timestamp: &commonpb.Timestamp{Data: 1_776_864_120_966_130},
		}

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("json", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, tx)
			require.NoError(t, err)
			require.True(t, handled)
		})

		// Uint256 must render as a plain number, not {"v0":...,"v1":...,...}
		require.Contains(t, out, `5000000000`)
		require.NotContains(t, out, `"v0"`)
		require.NotContains(t, out, `"v1"`)

		// Timestamp must render as an ISO 8601 string, not {"data":...}
		require.Contains(t, out, `"timestamp"`)
		require.Contains(t, out, "2026-")
		require.NotContains(t, out, `"data"`)
	})

	t.Run("transaction slice json renders properly", func(t *testing.T) {
		txs := []*commonpb.Transaction{
			{
				Id: 1,
				Postings: []*commonpb.Posting{
					{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewUint256FromUint64(100),
						Asset:       "EUR",
					},
				},
				Timestamp: &commonpb.Timestamp{Data: 1_000_000_000_000},
			},
		}

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("json", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, txs)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.NotContains(t, out, `"v0"`)
		require.NotContains(t, out, `"data"`)
	})

	t.Run("map string any with proto values", func(t *testing.T) {
		data := map[string]any{
			"info": &commonpb.NumscriptInfo{
				Name:      "x",
				CreatedAt: &commonpb.Timestamp{Data: 1000},
			},
			"items": []*commonpb.NumscriptInfo{
				{Name: "y", Version: "v1"},
			},
		}

		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)
		require.NoError(t, cmd.Flags().Set("json", "true"))

		out := captureStdout(t, func() {
			handled, err := cmdutil.EncodeStructured(cmd, data)
			require.NoError(t, err)
			require.True(t, handled)
		})

		require.Contains(t, out, `"createdAt"`)
		require.NotContains(t, out, `"created_at"`)
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

func TestEmitNextCursorHint(t *testing.T) {
	t.Parallel()

	newCmd := func() (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
		cmd := &cobra.Command{}
		cmdutil.AddOutputFlags(cmd)

		stdout := &bytes.Buffer{}
		stderr := &bytes.Buffer{}
		cmd.SetOut(stdout)
		cmd.SetErr(stderr)

		return cmd, stdout, stderr
	}

	t.Run("empty cursor — no output", func(t *testing.T) {
		t.Parallel()

		cmd, stdout, stderr := newCmd()
		cmdutil.EmitNextCursorHint(cmd, "")

		require.Empty(t, stdout.String())
		require.Empty(t, stderr.String())
	})

	t.Run("structured mode routes hint to stderr", func(t *testing.T) {
		t.Parallel()

		cmd, _, stderr := newCmd()
		require.NoError(t, cmd.Flags().Set("json", "true"))

		cmdutil.EmitNextCursorHint(cmd, "abc123")

		// stdout would carry the JSON payload from EncodeStructured in real
		// callers — keep it untouched here so `jq` / `yq` pipes stay lossless.
		require.Equal(t, "next_cursor=abc123\n", stderr.String())
	})

	t.Run("yaml mode also routes to stderr", func(t *testing.T) {
		t.Parallel()

		cmd, _, stderr := newCmd()
		require.NoError(t, cmd.Flags().Set("yaml", "true"))

		cmdutil.EmitNextCursorHint(cmd, "token-xyz")

		require.Equal(t, "next_cursor=token-xyz\n", stderr.String())
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
