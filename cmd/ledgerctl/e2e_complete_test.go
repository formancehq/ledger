package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type namedBucketServer struct {
	servicepb.UnimplementedBucketServiceServer
	ledger string
}

func (s namedBucketServer) ListLedgers(_ *servicepb.ListLedgersRequest, st grpc.ServerStreamingServer[commonpb.LedgerInfo]) error {
	return st.Send(&commonpb.LedgerInfo{Name: s.ledger})
}

func startServer(t *testing.T, ledgerName string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(srv, namedBucketServer{ledger: ledgerName})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// TestE2ECompletionUsesExplicitProfileNotActive reproduces the reported bug:
// with an active profile that differs from the one named by --profile, ledger
// completion must connect to the EXPLICIT profile's server, not the active one.
func TestE2ECompletionUsesExplicitProfileNotActive(t *testing.T) {
	activeAddr := startServer(t, "WRONG-active-ledger")
	devAddr := startServer(t, "aws-costs")

	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("LEDGERCTL_SERVER", "")
	base, err := os.UserConfigDir()
	require.NoError(t, err)
	dir := filepath.Join(base, "ledgerctl")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	cfg := `{"activeProfile":"acme","profiles":{` +
		`"acme":{"server":"` + activeAddr + `","insecure":true},` +
		`"dev":{"server":"` + devAddr + `","insecure":true}}}` + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o600))

	root := newRootCommand()
	root.SilenceErrors = true
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{cobra.ShellCompRequestCmd, "--profile", "dev", "accounts", "list", "--ledger", ""})
	require.NoError(t, root.Execute())

	t.Logf("completion output:\n%s", out.String())
	require.Contains(t, out.String(), "aws-costs", "completion must use the --profile dev server")
	require.NotContains(t, out.String(), "WRONG-active-ledger", "completion must NOT use the active profile's server")
}
