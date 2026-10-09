package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	ledgerplugin "github.com/formancehq/ledger/misc/fctl-plugin"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestSharedManifestHasDefaultNativeBindings(t *testing.T) {
	manifest, err := ledgerplugin.NewWithExecutor("test", nil).GetManifest(t.Context())
	require.NoError(t, err)
	root := NewCommand()
	require.Nil(t, root.Flag("shared-command-poc"))
	var visit func(pluginsdk.CommandSpec, []string)
	count := 0
	visit = func(spec pluginsdk.CommandSpec, parent []string) {
		path := append(append([]string{}, parent...), pluginsdk.CommandName(spec))
		if spec.Runnable {
			count++
			cmd, rest, err := root.Find(sharedBindings[joinSharedPath(path)])
			require.NoError(t, err)
			require.Empty(t, rest)
			require.Equal(t, joinSharedPath(path), cmd.Annotations["ledger-command"])
			require.NotNil(t, cmd.RunE)
		}
		for _, child := range spec.Subcommands {
			visit(child, path)
		}
	}
	visit(manifest.Root, nil)
	require.Equal(t, 31, count)
	for _, path := range [][]string{{"store", "backup"}, {"cluster"}, {"auth"}, {"profile"}, {"numscripts"}} {
		cmd, _, err := root.Find(path)
		require.NoError(t, err)
		require.Empty(t, cmd.Annotations["ledger-command"])
	}
}

func joinSharedPath(path []string) string {
	result := ""
	for _, part := range path {
		if result != "" {
			result += "/"
		}
		result += part
	}

	return result
}

func TestSharedBodyReaderHandlesSourcesAndCancellation(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.SetIn(bytes.NewBufferString(`{"precise":9007199254740993}`))
	body, err := readSharedBody(t.Context(), cmd, "-")
	require.NoError(t, err)
	require.Equal(t, `{"precise":9007199254740993}`, string(body))
	_, err = readSharedBody(t.Context(), cmd, "@/a-file-that-does-not-exist")
	require.Error(t, err)
	_, err = readSharedBody(t.Context(), cmd, `{broken`)
	require.ErrorContains(t, err, "invalid JSON")
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	cmd.SetIn(reader)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = readSharedBody(ctx, cmd, "-")
	require.ErrorIs(t, err, context.Canceled)
}

type sharedCommandServer struct {
	servicepb.UnimplementedBucketServiceServer

	apply         []*servicepb.ApplyRequest
	authorization []string
	partial       bool
}

func (s *sharedCommandServer) Apply(ctx context.Context, request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.apply = append(s.apply, request)
	md, _ := metadata.FromIncomingContext(ctx)
	s.authorization = md.Get("authorization")
	create := request.GetUnsigned().GetRequests()[0].GetCreateLedger()
	logs := []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: create.GetName(), Metadata: create.GetMetadata()}}}}}
	for _, req := range request.GetUnsigned().GetRequests()[1:] {
		logs = append(logs, &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: req.GetCreateIndex().GetId()}}}}}}}})
	}

	return &servicepb.ApplyResponse{Logs: logs}, nil
}

func (s *sharedCommandServer) ListLedgers(_ *servicepb.ListLedgersRequest, stream grpc.ServerStreamingServer[commonpb.LedgerInfo]) error {
	if err := stream.Send(&commonpb.LedgerInfo{Name: "partial"}); err != nil {
		return err
	}
	if s.partial {
		return status.Error(codes.DataLoss, "stream interrupted")
	}

	return nil
}

func TestDefaultLedgerCreateKeepsAtomicIndexesAndCleanJSON(t *testing.T) {
	server := startSharedCommandServer(t, &sharedCommandServer{})
	root := NewCommand()
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"ledgers", "create", "--name", "books", "--index", "reference", "--idempotency-key", "create-once", "--auth-token=test-token", "--json", "--insecure", "--server", server.address})
	require.NoError(t, root.ExecuteContext(t.Context()))
	require.True(t, json.Valid(stdout.Bytes()), stdout.String())
	require.Len(t, server.fixture.apply, 1)
	batch := server.fixture.apply[0].GetUnsigned()
	require.Equal(t, "create-once", batch.GetIdempotencyKey())
	require.Len(t, batch.GetRequests(), 2)
	require.Equal(t, "books", batch.GetRequests()[1].GetCreateIndex().GetLedger())
	require.Equal(t, []string{"Bearer test-token"}, server.fixture.authorization)
}

func TestDefaultLedgerListReturnsPartialJSONAndError(t *testing.T) {
	server := startSharedCommandServer(t, &sharedCommandServer{partial: true})
	root := NewCommand()
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"ledgers", "list", "--json", "--insecure", "--server", server.address})
	err := root.ExecuteContext(t.Context())
	require.Error(t, err)
	require.True(t, json.Valid(stdout.Bytes()), stdout.String())
	var result map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
	require.Contains(t, result, "partial")
}

type runningSharedCommandServer struct {
	address string
	fixture *sharedCommandServer
}

func startSharedCommandServer(t *testing.T, fixture *sharedCommandServer) runningSharedCommandServer {
	t.Helper()
	config := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", config)
	t.Setenv("APPDATA", config)
	for _, name := range []string{"LEDGERCTL_PROFILE", "LEDGERCTL_SERVER", "LEDGERCTL_TLS_CA_CERT", "LEDGERCTL_TLS_SERVER_NAME", "LEDGERCTL_SIGNING_KEY", "LEDGERCTL_RESPONSE_VERIFY_KEY", "LEDGERCTL_RESULT_FILE"} {
		t.Setenv(name, "")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, fixture)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		err := <-done
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})

	return runningSharedCommandServer{listener.Addr().String(), fixture}
}

func TestSharedLedgerNameConflictsAreRejectedBeforeExecution(t *testing.T) {
	for _, path := range []string{"create", "show", "delete"} {
		t.Run(path, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().String("name", "first", "")
			cmd.Flags().String("data", "", "")
			req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", path}, Args: []string{"second"}, Flags: map[string]string{}, ChangedFlags: map[string]bool{}}
			err := prepareSharedInput(t.Context(), cmd, nil, pluginsdk.CommandSpec{}, &req, nil)
			require.ErrorContains(t, err, "must agree")
		})
	}
}

func TestSharedSelectionConflictsAreRejectedBeforeExecution(t *testing.T) {
	t.Run("after and cursor", func(t *testing.T) {
		cmd := &cobra.Command{}
		req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "transactions", "list"}, Flags: map[string]string{"after": "1", "cursor": "eyJrZXkiOiIyIn0"}}
		err := prepareSharedInput(t.Context(), cmd, nil, pluginsdk.CommandSpec{}, &req, nil)
		require.ErrorContains(t, err, "--after and --cursor")
	})
	for _, operation := range []string{"delete", "inspect"} {
		for _, flag := range []string{"type", "target", "key"} {
			t.Run(operation+"/"+flag, func(t *testing.T) {
				cmd := &cobra.Command{}
				cmd.Flags().String(flag, "", "")
				require.NoError(t, cmd.Flags().Set(flag, "conflict"))
				req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "indexes", operation}, Args: []string{"metadata:TARGET_TYPE_ACCOUNT:foo"}, Flags: map[string]string{"ledger": "books"}}
				err := prepareSharedInput(t.Context(), cmd, nil, pluginsdk.CommandSpec{}, &req, nil)
				require.ErrorContains(t, err, "mutually exclusive")
			})
		}
	}
}
