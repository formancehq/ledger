//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
)

// TestOperatorIndexOwnership runs the operator's controller boundary tests
// against a real Ledger node and the real ledgerctl executable. The subprocess
// bridges the independent Go modules without introducing a module dependency.
func TestOperatorIndexOwnership(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs("../../..")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "build"), 0o755))
	buildDir, err := os.MkdirTemp(filepath.Join(root, "build"), "operator-index-test-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(buildDir)) })
	cli := filepath.Join(buildDir, "ledgerctl")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	build := exec.CommandContext(ctx, goTool, "build", "-o", cli, "./cmd/ledgerctl")
	build.Dir = root
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)

	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "operator-index-test", Ports: ports,
		WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard,
	})
	instruments = append(instruments, testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	endpoint := fmt.Sprintf("localhost:%d", ports.GRPC())
	conn, err := grpc.NewClient(endpoint, grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 10*time.Second, 10*time.Millisecond)

	tests := exec.CommandContext(ctx, goTool, "test", "-race", "./internal/controller", "-run", "^TestLedgerIndexOwnershipReal", "-count=1", "-v")
	tests.Dir = filepath.Join(root, "misc/operator")
	tests.Env = append(os.Environ(), "LEDGER_INDEX_TEST_SERVER="+endpoint, "LEDGERCTL_BINARY="+cli)
	output, err = tests.CombinedOutput()
	t.Logf("%s", output)
	require.NoError(t, err)
}
