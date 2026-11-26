package main

import (
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type signingConfigServer struct {
	servicepb.UnimplementedBucketServiceServer

	requireSignatures atomic.Bool
	fail              atomic.Bool
	calls             atomic.Int32
}

func (s *signingConfigServer) GetSigningConfig(context.Context, *servicepb.GetSigningConfigRequest) (*servicepb.GetSigningConfigResponse, error) {
	s.calls.Add(1)
	if s.fail.Load() {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}

	return &servicepb.GetSigningConfigResponse{RequireSignatures: s.requireSignatures.Load()}, nil
}

// Sequential because command execution redirects process-global stdout.
func TestSigningGetConfigCommand(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("APPDATA", tmp)
	t.Setenv("LOCALAPPDATA", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")

	cmd, remaining, err := newRootCommand().Find([]string{"signing", "get-config"})
	require.NoError(t, err)
	require.Empty(t, remaining)
	require.Equal(t, "ledgerctl signing get-config", cmd.CommandPath())
	require.NoError(t, cmd.Args(cmd, nil))
	require.Error(t, cmd.Args(cmd, []string{"extra"}))
	suggestions, directive := cmd.ValidArgsFunction(cmd, nil, "")
	require.Empty(t, suggestions)
	require.Equal(t, cobra.ShellCompDirectiveNoFileComp, directive)

	fixture := &signingConfigServer{}
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

	args := func(extra ...string) []string {
		return append([]string{
			"signing", "get-config", "--server", listener.Addr().String(), "--insecure",
			"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
			"--signing-key=", "--response-verify-key=", "--auth-token=test-token",
		}, extra...)
	}

	for _, tc := range []struct {
		requireSignatures bool
		state             string
	}{{false, "disabled"}, {true, "enabled"}} {
		fixture.requireSignatures.Store(tc.requireSignatures)

		var body map[string]any
		require.NoError(t, json.Unmarshal([]byte(executeEventsCommand(t, args("--json"))), &body))
		require.Equal(t, map[string]any{"requireSignatures": tc.requireSignatures}, body)

		require.Contains(t, executeEventsCommand(t, args()), tc.state)
	}

	fixture.fail.Store(true)
	before := fixture.calls.Load()
	root := newRootCommand()
	root.SilenceErrors = true
	root.SilenceUsage = true
	root.SetArgs(args("--json"))
	require.Error(t, root.ExecuteContext(t.Context()))
	require.Equal(t, before+1, fixture.calls.Load(), "the error must come from the RPC, not earlier setup")
}
