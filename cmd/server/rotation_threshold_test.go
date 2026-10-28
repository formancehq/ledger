package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/pkg/network"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

func TestLoadConfigRotationThreshold(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		args []string
		want uint64
	}{
		{name: "omitted", want: 1000},
		{name: "explicit zero retains default sentinel", args: []string{"--cache-rotation-threshold=0"}},
		{name: "explicit positive", args: []string{"--cache-rotation-threshold=42"}, want: 42},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			command := NewRunCommand()
			require.NoError(t, command.ParseFlags(append([]string{"--node-id=1", "--cluster-id=rotation-threshold-test"}, tt.args...)))
			cfg, err := LoadConfig(t.Context(), command)
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.RaftConfig.RotationThreshold)
			require.NoError(t, cfg.Validate())
		})
	}
}

// Exercise the production command and leadership observer across two process
// lifetimes. A full ClusterConfig from the RPC proves the leadership proposal
// was persisted; its minimal fallback would expose only the cache threshold.
func TestZeroRotationThresholdServerRestart(t *testing.T) {
	const helperEnv = "LEDGER_ROTATION_THRESHOLD_HELPER"
	if os.Getenv(helperEnv) == "1" {
		runRotationThresholdHelper(t)

		return
	}
	t.Parallel()

	dataDir := t.TempDir()
	raftAddress := "127.0.0.1:0"
	for attempt := range 2 {
		t.Run(fmt.Sprintf("start-%d", attempt+1), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestZeroRotationThresholdServerRestart$")
			command.Env = []string{helperEnv + "=1", "LEDGER_ROTATION_THRESHOLD_DIR=" + dataDir, "LEDGER_ROTATION_THRESHOLD_RAFT=" + raftAddress}
			output, err := command.StdoutPipe()
			require.NoError(t, err)
			command.Stderr = command.Stdout
			require.NoError(t, command.Start())
			waited := false
			var exitErr error
			wait := func() error {
				if !waited {
					exitErr = command.Wait()
					waited = true
				}

				return exitErr
			}
			defer func() {
				cancel()
				_ = wait() // Reap the child if an earlier assertion stopped the test.
			}()

			addresses := make(chan []string, 1)
			captured := make(chan string, 1)
			go func() {
				var log bytes.Buffer
				scanner := bufio.NewScanner(output)
				for scanner.Scan() {
					line := scanner.Text()
					fmt.Fprintln(&log, line)
					if value, ok := strings.CutPrefix(line, "ROTATION_THRESHOLD_ADDRESSES="); ok {
						addresses <- strings.Fields(value)
					}
				}
				if err := scanner.Err(); err != nil {
					fmt.Fprintf(&log, "reading server output: %v\n", err)
				}
				captured <- log.String()
			}()

			var serviceAddress string
			select {
			case bound := <-addresses:
				require.Len(t, bound, 2)
				serviceAddress, raftAddress = bound[0], bound[1]
			case log := <-captured:
				t.Fatalf("server exited before binding listeners:\n%s", log)
			case <-ctx.Done():
				t.Fatal("server did not bind listeners")
			}
			connection, err := grpc.NewClient(serviceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpcprotocol.ClientOption())
			require.NoError(t, err)
			defer func() { require.NoError(t, connection.Close()) }()
			client := clusterpb.NewClusterServiceClient(connection)
			var committedThreshold uint64
			ready := assert.Eventually(t, func() bool {
				requestCtx, cancelRequest := context.WithTimeout(ctx, time.Second)
				defer cancelRequest()
				state, err := client.GetClusterState(requestCtx, &clusterpb.GetClusterStateRequest{NodeId: 1})
				if err != nil || state.GetClusterConfig().GetBloomVolumes() == nil {
					return false
				}
				committedThreshold = state.GetClusterConfig().GetRotationThreshold()

				return true
			}, 20*time.Second, 20*time.Millisecond)
			if ready {
				require.NoError(t, command.Process.Signal(os.Interrupt))
			} else {
				cancel()
			}
			log := <-captured
			exitErr = wait()
			require.True(t, ready, "server must persist a full cluster configuration:\n%s", log)
			require.Equal(t, uint64(1000), committedThreshold, log)
			require.NoError(t, exitErr, log)
			require.NotContains(t, log, "panic:")
		})
		if t.Failed() {
			break
		}
	}
}

func runRotationThresholdHelper(t *testing.T) {
	t.Helper()
	listen := func(address string) net.Listener {
		listener, err := net.Listen("tcp", address)
		require.NoError(t, err)

		return listener
	}
	httpListener := listen("127.0.0.1:0")
	serviceListener := listen("127.0.0.1:0")
	raftListener := listen(os.Getenv("LEDGER_ROTATION_THRESHOLD_RAFT"))
	fmt.Printf("ROTATION_THRESHOLD_ADDRESSES=%s %s\n", serviceListener.Addr(), raftListener.Addr())
	command := NewRunCommandWithBindings(network.Bindings{HTTP: httpListener, Service: serviceListener, Raft: raftListener})
	dataDir := os.Getenv("LEDGER_ROTATION_THRESHOLD_DIR")
	command.SetArgs([]string{
		"--node-id=1", "--cluster-id=rotation-threshold-test", "--bootstrap", "--cache-rotation-threshold=0",
		"--bloom-volumes-expected-keys=42",
		"--data-dir=" + filepath.Join(dataDir, "data"), "--wal-dir=" + filepath.Join(dataDir, "wal"),
		"--bind-addr=" + raftListener.Addr().String(),
	})
	service.Execute(command)
}
