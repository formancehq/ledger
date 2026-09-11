package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/pkg/network"
)

const httpFailureHelperEnv = "LEDGER_HTTP_FAILURE_HELPER"
const httpFailureSentinel = "EN-1995 injected permanent accept failure"

// Exercise the production command, module wiring, service runner and process
// exit together. A helper process isolates service.Execute's os.Exit call.
func TestHTTPServeFailureStopsServerProcess(t *testing.T) {
	if mode := os.Getenv(httpFailureHelperEnv); mode != "" {
		runHTTPFailureHelper(t, mode)

		return
	}
	t.Parallel()

	for _, mode := range []string{"normal", "restore"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHTTPServeFailureStopsServerProcess$")
			command.Env = append(os.Environ(), httpFailureHelperEnv+"="+mode, "LEDGER_HTTP_FAILURE_DIR="+t.TempDir())
			input, err := command.StdinPipe()
			require.NoError(t, err)
			output, err := command.StdoutPipe()
			require.NoError(t, err)
			command.Stderr = command.Stdout
			require.NoError(t, command.Start())
			waited := false
			var waitErr error
			wait := func() error {
				if !waited {
					waitErr = command.Wait()
					waited = true
				}

				return waitErr
			}
			defer func() {
				cancel()
				_ = wait() // Reap the helper even when an earlier assertion fails.
			}()

			ready := make(chan string, 1)
			captured := make(chan string, 1)
			go func() {
				var log bytes.Buffer
				scanner := bufio.NewScanner(output)
				for scanner.Scan() {
					line := scanner.Text()
					fmt.Fprintln(&log, line)
					if address, ok := strings.CutPrefix(line, "HTTP_FAILURE_ADDRESS="); ok {
						ready <- address
					}
				}
				captured <- log.String()
			}()

			select {
			case address := <-ready:
				path := "/readyz"
				if mode == "restore" {
					path = "/health"
				}
				client := &http.Client{Timeout: time.Second}
				require.Eventually(t, func() bool {
					response, requestErr := client.Get("http://" + address + path)
					if requestErr != nil {
						return false
					}
					_ = response.Body.Close() // Only the readiness status is needed.

					return response.StatusCode == http.StatusOK
				}, 20*time.Second, 20*time.Millisecond, "server must become ready before injecting the failure")
				_, err = io.WriteString(input, "fail\n")
				require.NoError(t, err)
			case log := <-captured:
				_ = wait()
				t.Fatalf("server exited before reaching HTTP Accept:\n%s", log)
			case <-ctx.Done():
				_ = wait()
				t.Fatalf("server did not reach HTTP Accept:\n%s", <-captured)
			}
			_ = input.Close() // The injected failure needs only the first input byte.
			log := <-captured
			err = wait()
			require.NoError(t, ctx.Err(), "server remained alive after permanent HTTP failure:\n%s", log)
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, "server must exit unsuccessfully:\n%s", log)
			require.Equal(t, 1, exitErr.ExitCode(), log)
			require.Contains(t, log, httpFailureSentinel)
			require.Contains(t, log, "App stopped!", "service runner must finish its shutdown path")
			if mode == "normal" {
				require.Contains(t, log, "Raft cluster stopped successfully", "HTTP failure must run application cleanup")
			}
		})
	}
}

type controlledHTTPFailureListener struct {
	net.Listener

	failed atomic.Bool
}

func (l *controlledHTTPFailureListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil && l.failed.Load() {
		return nil, errors.New(httpFailureSentinel)
	}

	return connection, err
}

func runHTTPFailureHelper(t *testing.T, mode string) {
	t.Helper()
	listen := func() net.Listener {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		return listener
	}
	httpListener, serviceListener, raftListener := listen(), listen(), listen()
	controlled := &controlledHTTPFailureListener{Listener: httpListener}
	go func() {
		var control [1]byte
		if _, err := os.Stdin.Read(control[:]); err == nil {
			controlled.failed.Store(true)
			_ = controlled.Close() // Force Accept to return the injected permanent failure.
		}
	}()
	fmt.Println("HTTP_FAILURE_ADDRESS=" + httpListener.Addr().String())
	command := NewRunCommandWithBindings(network.Bindings{
		HTTP: controlled, Service: serviceListener, Raft: raftListener,
	})
	dataDir := os.Getenv("LEDGER_HTTP_FAILURE_DIR")
	args := []string{"--node-id=1", "--cluster-id=http-failure-test", "--data-dir=" + filepath.Join(dataDir, "data"), "--wal-dir=" + filepath.Join(dataDir, "wal"), "--bind-addr=" + raftListener.Addr().String()}
	if mode == "restore" {
		args = append(args, "--restore")
	} else {
		args = append(args, "--bootstrap")
	}
	command.SetArgs(args)
	service.Execute(command)
}
