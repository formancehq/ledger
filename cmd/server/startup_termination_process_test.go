package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// Use the real executable: a test-binary helper would bypass main, service.Execute,
// and the process signal handlers whose interaction stalled learner registration.
func TestSIGTERMDuringLearnerRegistrationStopsServerProcess(t *testing.T) {
	t.Parallel()

	repository, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	buildDir := filepath.Join(t.TempDir(), "build")
	require.NoError(t, os.MkdirAll(buildDir, 0o755))
	binary := filepath.Join(buildDir, "ledger")
	buildCtx, cancelBuild := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".")
	build.Dir = repository
	buildOutput, err := build.CombinedOutput()
	require.NoError(t, err, "build production executable: %s", buildOutput)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := &unavailableStartupBootstrapServer{address: listener.Addr().String(), attempts: make(chan struct{}, 32)}
	bootstrapServer := grpc.NewServer()
	clusterbootstrappb.RegisterClusterBootstrapServiceServer(bootstrapServer, peer)
	go func() {
		_ = bootstrapServer.Serve(listener) // Stop below owns the listener's lifecycle.
	}()
	t.Cleanup(bootstrapServer.Stop)

	storage := t.TempDir()
	var instanceID []byte
	for generation := range 2 {
		t.Run(fmt.Sprintf("same-path-start-%d", generation+1), func(t *testing.T) {
			var output bytes.Buffer
			command := exec.Command(binary, "run",
				"--node-id=2", "--cluster-id=startup-termination-test",
				"--join="+peer.address, "--bind-addr=127.0.0.1:0",
				"--grpc-port=0", "--http-port=0", "--total-stop-timeout=2s",
				"--wal-dir="+filepath.Join(storage, "wal"),
				"--data-dir="+filepath.Join(storage, "data"),
			)
			// Isolate the executable from developer LEDGER_* configuration and
			// telemetry exporters. No harness context ever cancels this process.
			command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
			command.Stdout, command.Stderr = &output, &output
			require.NoError(t, command.Start())
			stopped := make(chan struct{})
			var waitErr error
			go func() {
				waitErr = command.Wait()
				close(stopped)
			}()
			t.Cleanup(func() {
				select {
				case <-stopped:
				default:
					_ = command.Process.Kill() // Failure cleanup only; never supplies a passing exit.
					<-stopped
				}
				if t.Failed() {
					t.Logf("server output:\n%s", output.String())
				}
			})

			// Cold executable startup can be slow on busy builders; the signal
			// assertion below retains its independent, short deadline.
			ready := time.NewTimer(90 * time.Second)
			defer ready.Stop()
			for range 2 {
				select {
				case <-peer.attempts:
				case <-stopped:
					t.Fatalf("server exited before repeated learner registration: %v\n%s", waitErr, output.String())
				case <-ready.C:
					t.Fatal("server did not make repeated learner registration attempts")
				}
			}
			require.NoError(t, command.Process.Signal(syscall.SIGTERM))
			// The production budget is two seconds. Allow three more for process
			// scheduling, while keeping the harness kill distinct from success.
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			select {
			case <-stopped:
				require.NoError(t, waitErr, "SIGTERM must finish clean rollback, not fail startup:\n%s", output.String())
			case <-deadline.C:
				t.Fatal("server remained alive after SIGTERM during persistent learner registration")
			}
			require.False(t, wal.IsClusterJoined(filepath.Join(storage, "wal")), "unavailable registration must not create the joined marker")
			identity, err := wal.EnsureInstanceID(filepath.Join(storage, "wal"))
			require.NoError(t, err)
			if generation == 0 {
				instanceID = identity
			} else {
				require.Equal(t, instanceID, identity, "same-path restart must preserve node identity")
			}
		})
		if t.Failed() {
			return
		}
		// Discard any additional attempts from the exited generation so the
		// restart must independently reach repeated registration before SIGTERM.
		for len(peer.attempts) > 0 {
			<-peer.attempts
		}
	}
}

type unavailableStartupBootstrapServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer

	address  string
	attempts chan struct{}
}

func (s *unavailableStartupBootstrapServer) GetPeers(context.Context, *clusterbootstrappb.GetPeersRequest) (*clusterbootstrappb.GetPeersResponse, error) {
	return &clusterbootstrappb.GetPeersResponse{Peers: []*clusterbootstrappb.PeerInfo{{
		Id: 1, RaftAddress: s.address, ServiceAddress: s.address,
		InstanceId: []byte("0123456789abcdef"),
	}}}, nil
}

func (s *unavailableStartupBootstrapServer) JoinAsLearner(context.Context, *clusterbootstrappb.JoinAsLearnerRequest) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
	select {
	case s.attempts <- struct{}{}:
	default:
	}

	return nil, status.Error(codes.Unavailable, "controlled learner registration outage")
}
