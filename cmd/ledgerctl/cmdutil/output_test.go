package cmdutil_test

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Sequential because captureStdout mutates os.Stdout.
func TestEncodeStructured_ExactNumbers(t *testing.T) {
	response := &servicepb.GetTransactionResponse{Transaction: &commonpb.Transaction{
		Id: 9007199254740993,
		Metadata: map[string]*commonpb.MetadataValue{
			"positive":      commonpb.NewUintValue(9007199254740993),
			"negative":      commonpb.NewIntValue(-9007199254740993),
			"maxUint":       commonpb.NewUintValue(math.MaxUint64),
			"minInt":        commonpb.NewIntValue(math.MinInt64),
			"numericString": commonpb.NewStringValue("9007199254740993"),
		},
	}}
	for _, format := range []string{"json", "yaml"} {
		for _, tc := range []struct {
			name   string
			data   any
			prefix []string
		}{
			{name: "response", data: response},
			{name: "proto slice", data: []*servicepb.GetTransactionResponse{response}, prefix: []string{"0"}},
			{name: "proto map", data: map[string]*servicepb.GetTransactionResponse{"item": response}, prefix: []string{"item"}},
			{name: "mixed map", data: map[string]any{"item": response}, prefix: []string{"item"}},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				cmd := &cobra.Command{}
				cmdutil.AddOutputFlags(cmd)
				require.NoError(t, cmd.Flags().Set(format, "true"))
				out := captureStdout(t, func() {
					handled, err := cmdutil.EncodeStructured(cmd, tc.data)
					require.NoError(t, err)
					require.True(t, handled)
				})
				// Nodes preserve scalar spelling and tags without converting to float64.
				var document yaml.Node
				require.NoError(t, yaml.Unmarshal([]byte(out), &document))
				transaction := outputNodeAt(t, document.Content[0], append(tc.prefix, "transaction")...)
				for key, want := range map[string]string{
					"positive": "9007199254740993", "negative": "-9007199254740993",
					"maxUint": "18446744073709551615", "minInt": "-9223372036854775808",
				} {
					t.Run(key, func(t *testing.T) {
						node := outputNodeAt(t, transaction, "metadata", key)
						require.Equal(t, "!!int", node.Tag)
						require.Equal(t, want, node.Value)
					})
				}
				require.Equal(t, "9007199254740993", outputNodeAt(t, transaction, "id").Value)
				require.Equal(t, "!!str", outputNodeAt(t, transaction, "metadata", "numericString").Tag)
			})
		}
	}
}

func outputNodeAt(t *testing.T, node *yaml.Node, path ...string) *yaml.Node {
	t.Helper()
	for _, key := range path {
		if node.Kind == yaml.SequenceNode {
			require.Equal(t, "0", key)
			require.NotEmpty(t, node.Content)
			node = node.Content[0]

			continue
		}
		require.Equal(t, yaml.MappingNode, node.Kind)
		var child *yaml.Node
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Value == key {
				child = node.Content[i+1]

				break
			}
		}
		require.NotNil(t, child, "missing key %q", key)
		node = child
	}

	return node
}

// Sequential because captureStdout mutates os.Stdout.
func TestEncodeStructured_YAMLScalarKinds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		data  any
		tag   string
		value string
	}{
		{name: "uint256", data: &commonpb.Uint256{V3: 256}, tag: "!!int", value: "1606938044258990275541962092341162602522202993782792835301376"},
		{name: "fraction", data: json.Number("1.25"), tag: "!!float", value: "1.25"},
		{name: "exponent", data: json.Number("1e+30"), tag: "!!float", value: "1e+30"},
		{name: "string", data: "9007199254740993", tag: "!!str", value: "9007199254740993"},
		{name: "boolean", data: true, tag: "!!bool", value: "true"},
		{name: "null", data: nil, tag: "!!null", value: "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmdutil.AddOutputFlags(cmd)
			require.NoError(t, cmd.Flags().Set("yaml", "true"))
			out := captureStdout(t, func() {
				handled, err := cmdutil.EncodeStructured(cmd, tc.data)
				require.NoError(t, err)
				require.True(t, handled)
			})
			var document yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(out), &document))
			require.Len(t, document.Content, 1)
			require.Equal(t, yaml.ScalarNode, document.Content[0].Kind)
			require.Equal(t, tc.tag, document.Content[0].Tag)
			require.Equal(t, tc.value, document.Content[0].Value)
		})
	}
}

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

// Sequential because captureStdout mutates os.Stdout, like TestEncodeStructured.
func TestEncodeStructured_LedgerLog(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			log := &commonpb.Log{
				Sequence: 7,
				Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: &commonpb.LedgerLog{Id: 3, Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &commonpb.DeletedMetadata{
								Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 0}},
								Key:    "note",
							}},
						}},
					},
				}},
			}
			cmd := &cobra.Command{}
			cmdutil.AddOutputFlags(cmd)
			require.NoError(t, cmd.Flags().Set(format, "true"))
			out := captureStdout(t, func() {
				handled, err := cmdutil.EncodeStructured(cmd, log)
				require.NoError(t, err)
				require.True(t, handled)
			})

			var response struct {
				Sequence uint64 `json:"sequence" yaml:"sequence"`
				Payload  struct {
					Apply struct {
						LedgerName string `json:"ledgerName" yaml:"ledgerName"`
						Log        struct {
							ID   uint64         `json:"id"   yaml:"id"`
							Type string         `json:"type" yaml:"type"`
							Data map[string]any `json:"data" yaml:"data"`
						} `json:"log" yaml:"log"`
					} `json:"apply" yaml:"apply"`
				} `json:"payload" yaml:"payload"`
			}
			if format == "json" {
				require.NoError(t, json.Unmarshal([]byte(out), &response))
			} else {
				require.NoError(t, yaml.Unmarshal([]byte(out), &response))
			}
			require.Equal(t, uint64(7), response.Sequence)
			require.Equal(t, "orders", response.Payload.Apply.LedgerName)
			gotLog := response.Payload.Apply.Log
			require.Equal(t, uint64(3), gotLog.ID)
			require.Equal(t, "DELETE_METADATA", gotLog.Type)
			require.Len(t, gotLog.Data, 3)
			require.Equal(t, "TRANSACTION", gotLog.Data["targetType"])
			require.Equal(t, "note", gotLog.Data["key"])
			require.Contains(t, gotLog.Data, "targetId", "transaction ID zero must remain present")
			require.Zero(t, gotLog.Data["targetId"])
		})
	}
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
