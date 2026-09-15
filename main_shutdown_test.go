//go:build !windows

package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/cmd/server"
	httpadapter "github.com/formancehq/ledger/v3/internal/adapter/http"
	"github.com/formancehq/ledger/v3/pkg/testserver"
)

const shutdownHelperProcess = "LEDGER_SHUTDOWN_HELPER_PROCESS"

// TestSIGTERMShutdownDeadline exercises the real command and signal handling.
// A 100 Continue response proves that the HTTP handler started reading the body
// before SIGTERM; merely connecting would not exercise an active-handler drain.
func TestSIGTERMShutdownDeadline(t *testing.T) {
	if os.Getenv(shutdownHelperProcess) == "1" {
		lease := testserver.AllocateNodeLease()
		ports := lease.Ports()
		cmd := server.NewRunCommandWithBindings(lease.NextGeneration())
		cmd.SetArgs([]string{
			"--node-id=1", "--bootstrap", "--cluster-id=shutdown-test",
			"--http-port=" + strconv.Itoa(ports.HTTP()),
			"--grpc-port=" + strconv.Itoa(ports.GRPC()),
			fmt.Sprintf("--bind-addr=127.0.0.1:%d", ports.Raft()),
			"--wal-dir=" + filepath.Join(os.Getenv("LEDGER_SHUTDOWN_DATA"), "wal"),
			"--data-dir=" + filepath.Join(os.Getenv("LEDGER_SHUTDOWN_DATA"), "data"),
			"--total-stop-timeout=1s", "--grace-period=0s",
			"--raft-tick-interval=10ms",
			"--bloom-volumes-expected-keys=10000",
			"--bloom-metadata-expected-keys=1000",
			"--bloom-references-expected-keys=1000",
		})
		ctx := service.ContextWithLifecycle(context.Background())
		cmd.SetContext(ctx)
		go func() {
			<-service.Ready(ctx)
			ready := os.NewFile(3, "ready")
			_, err := fmt.Fprintf(ready, "127.0.0.1:%d\n", ports.HTTP())
			if err != nil {
				panic(err)
			}
			if err := ready.Close(); err != nil {
				panic(err)
			}
		}()
		service.Execute(cmd)

		return
	}

	t.Parallel()
	for _, stalled := range []bool{true, false} {
		name := "completed_body"
		if stalled {
			name = "stalled_body"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			readyReader, readyWriter, err := os.Pipe()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, readyReader.Close()) })
			output, err := os.Create(filepath.Join(t.TempDir(), "server.log"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, output.Close()) })
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSIGTERMShutdownDeadline$")
			cmd.Env = []string{shutdownHelperProcess + "=1", "LEDGER_SHUTDOWN_DATA=" + t.TempDir()}
			cmd.ExtraFiles = []*os.File{readyWriter}
			cmd.Stdout, cmd.Stderr = output, output
			require.NoError(t, cmd.Start())
			require.NoError(t, readyWriter.Close())
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			waited := false
			t.Cleanup(func() {
				cancel()
				// Wait completes before reading diagnostics or removing node files.
				if !waited {
					<-done
				}
				if t.Failed() {
					logs, readErr := os.ReadFile(output.Name())
					require.NoError(t, readErr)
					t.Logf("server output:\n%s", logs)
				}
			})
			require.NoError(t, readyReader.SetReadDeadline(time.Now().Add(20*time.Second)))
			address, err := bufio.NewReader(readyReader).ReadString('\n')
			require.NoError(t, err, "server did not finish startup")
			address = address[:len(address)-1]

			conn, err := net.DialTimeout("tcp", address, 5*time.Second)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
			_, err = fmt.Fprintf(conn, "POST "+httpadapter.APIVersionPrefix+"/shutdown-test HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: 2\r\nExpect: 100-continue\r\n\r\n", address)
			require.NoError(t, err)
			reader := bufio.NewReader(conn)
			response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodPost})
			require.NoError(t, err)
			require.Equal(t, http.StatusContinue, response.StatusCode)
			require.NoError(t, response.Body.Close())
			if !stalled {
				// Finish the same body read so normal HTTP draining remains covered.
				_, err = fmt.Fprint(conn, "{}")
				require.NoError(t, err)
				response, err = http.ReadResponse(reader, &http.Request{Method: http.MethodPost})
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
			}

			require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
			select {
			case err := <-done:
				waited = true
				if stalled {
					require.Error(t, err)
					require.Equal(t, 1, cmd.ProcessState.ExitCode())
					logs, readErr := os.ReadFile(output.Name())
					require.NoError(t, readErr)
					require.Contains(t, string(logs), "context deadline exceeded")
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("SIGTERM did not stop the process within the 1s stop budget plus 4s scheduling tolerance")
			}
		})
	}
}
