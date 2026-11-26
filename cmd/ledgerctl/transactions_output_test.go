package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type transactionOutputServer struct {
	servicepb.UnimplementedBucketServiceServer

	response *servicepb.GetTransactionResponse
	requests chan *servicepb.GetTransactionRequest
}

func (s *transactionOutputServer) GetTransaction(_ context.Context, request *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	s.requests <- request

	return s.response, nil
}

// Sequential because the registered command changes pterm writers and the
// stdout capture and isolated profile configuration modify process globals.
func TestTransactionsGetStructuredOutputPreservesIntegers(t *testing.T) {
	const positive = uint64(9_007_199_254_740_993)
	const negative = int64(-9_007_199_254_740_993)

	// Seed the transport fixture with typed protobuf values. Passing through
	// HTTP/JSON on the way in could round the input before this CLI regression.
	fixture := &transactionOutputServer{
		response: &servicepb.GetTransactionResponse{
			Transaction: &commonpb.Transaction{
				Id: positive,
				Metadata: map[string]*commonpb.MetadataValue{
					"positive": commonpb.NewUintValue(positive),
					"negative": commonpb.NewIntValue(negative),
				},
			},
		},
		requests: make(chan *servicepb.GetTransactionRequest, 2),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, fixture)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveResult)
	})

	configDir := t.TempDir()
	t.Setenv("HOME", configDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("LEDGERCTL_PROFILE", "")

	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			root := newRootCommand()
			root.SetArgs([]string{
				"transactions", "get", "9007199254740993", "--ledger", "precise",
				"--server", listener.Addr().String(), "--insecure",
				"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
				"--signing-key=", "--response-verify-key=",
				"--auth-token=test-token", "--" + format,
			})

			stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
			require.NoError(t, err)
			originalStdout := os.Stdout
			os.Stdout = stdout
			t.Cleanup(func() {
				os.Stdout = originalStdout
				pterm.SetDefaultOutput(originalStdout)
				require.NoError(t, stdout.Close())
			})
			require.NoError(t, root.ExecuteContext(t.Context()))
			_, err = stdout.Seek(0, io.SeekStart)
			require.NoError(t, err)
			output, err := io.ReadAll(stdout)
			require.NoError(t, err)

			select {
			case request := <-fixture.requests:
				require.Equal(t, "precise", request.GetLedger())
				require.Equal(t, positive, request.GetTransactionId())
			default:
				t.Fatal("registered transactions get command did not call GetTransaction")
			}

			if format == "json" {
				decoder := json.NewDecoder(bytes.NewReader(output))
				decoder.UseNumber()
				var response map[string]any
				require.NoError(t, decoder.Decode(&response))
				transaction := response["transaction"].(map[string]any)
				metadata := transaction["metadata"].(map[string]any)
				assert.Equal(t, json.Number("9007199254740993"), transaction["id"])
				assert.Equal(t, json.Number("9007199254740993"), metadata["positive"])
				assert.Equal(t, json.Number("-9007199254740993"), metadata["negative"])

				return
			}

			var document yaml.Node
			require.NoError(t, yaml.Unmarshal(output, &document))
			require.Len(t, document.Content, 1)
			transaction := transactionOutputYAMLField(t, document.Content[0], "transaction")
			metadata := transactionOutputYAMLField(t, transaction, "metadata")
			for _, expected := range []struct {
				node  *yaml.Node
				value string
			}{
				{transactionOutputYAMLField(t, transaction, "id"), "9007199254740993"},
				{transactionOutputYAMLField(t, metadata, "positive"), "9007199254740993"},
				{transactionOutputYAMLField(t, metadata, "negative"), "-9007199254740993"},
			} {
				assert.Equal(t, yaml.ScalarNode, expected.node.Kind)
				assert.Equal(t, "!!int", expected.node.Tag, "integer %s must remain a numeric YAML scalar", expected.value)
				assert.Equal(t, expected.value, expected.node.Value)
			}
		})
	}
}

func transactionOutputYAMLField(t *testing.T, mapping *yaml.Node, key string) *yaml.Node {
	t.Helper()
	require.Equal(t, yaml.MappingNode, mapping.Kind)
	for i := 0; i < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			return mapping.Content[i+1]
		}
	}
	t.Fatalf("missing YAML field %q", key)

	return nil
}
