//go:build darwin || linux

package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Run terminal input in a separate process: pterm's keyboard reader owns global
// terminal state and opens /dev/tty rather than Cobra's input reader.
func TestTransactionsCreateInteractiveAmount(t *testing.T) {
	if serverAddress := os.Getenv("LEDGER_TEST_POSTING_SERVER"); serverAddress != "" {
		root := newRootCommand()
		root.SetArgs([]string{
			"transactions", "create", "--ledger", "fresh",
			"--server", serverAddress, "--insecure", "--json",
			"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
			"--signing-key=", "--response-verify-key=", "--auth-token=test-token",
		})
		require.ErrorContains(t, root.ExecuteContext(t.Context()), os.Getenv("LEDGER_TEST_POSTING_ERROR"))
		fmt.Println("posting input rejected")

		return
	}

	for _, tc := range []struct {
		name      string
		amount    string
		wantError string
	}{
		{name: "overflow", amount: "115792089237316195423570985008687907853269984665640564039457584007913129639936", wantError: "amount must not exceed 2^256 - 1"},
		{name: "negative", amount: "-1", wantError: "must be a positive integer"},
		{name: "zero", amount: "0", wantError: "must be a positive integer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := &postingAmountServer{requests: make(chan *servicepb.ApplyRequest, 2)}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			servicepb.RegisterBucketServiceServer(server, fixture)
			serveResult := make(chan error, 1)
			go func() { serveResult <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				if err := <-serveResult; err != nil {
					require.ErrorIs(t, err, grpc.ErrServerStopped)
				}
			})

			terminal, slave, err := pty.Open()
			require.NoError(t, err)
			closeTerminal := sync.OnceFunc(func() {
				require.NoError(t, terminal.Close())
				require.NoError(t, slave.Close())
			})
			t.Cleanup(closeTerminal)
			initialState, err := term.GetState(int(slave.Fd()))
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTransactionsCreateInteractiveAmount$", "-test.v")
			configDir := t.TempDir()
			child.Env = append(os.Environ(),
				"HOME="+configDir, "XDG_CONFIG_HOME="+configDir, "APPDATA="+configDir, "LEDGERCTL_PROFILE=",
				"LEDGER_TEST_POSTING_SERVER="+listener.Addr().String(), "LEDGER_TEST_POSTING_ERROR="+tc.wantError,
			)
			child.Stdin, child.Stdout, child.Stderr = slave, slave, slave
			child.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
			require.NoError(t, child.Start())
			childDone := make(chan struct{})
			var childError error
			go func() {
				childError = child.Wait()
				close(childDone)
			}()
			defer func() {
				cancel()
				closeTerminal() // Unblock terminal I/O before waiting for the killed subprocess.
				<-childDone     // Always reap the subprocess, including on expectation failure.
			}()

			output := make(chan byte)
			go func() {
				defer close(output)
				reader := bufio.NewReader(terminal)
				for {
					b, err := reader.ReadByte()
					if err != nil {
						return
					}
					select {
					case output <- b:
					case <-ctx.Done():
						return
					}
				}
			}()
			var transcript strings.Builder
			matchedUntil := 0
			expect := func(text string) {
				t.Helper()
				for !strings.Contains(transcript.String()[matchedUntil:], text) {
					if text == "posting input rejected" && strings.Contains(transcript.String()[matchedUntil:], "Asset (e.g.") {
						t.Fatalf("invalid amount reached the asset prompt instead of returning a validation error:\n%s", transcript.String())
					}
					select {
					case b, ok := <-output:
						require.True(t, ok, "terminal closed waiting for %q: %s", text, transcript.String())
						transcript.WriteByte(b)
					case <-ctx.Done():
						t.Fatalf("waiting for %q: %v\n%s", text, ctx.Err(), transcript.String())
					}
				}
				matchedUntil += strings.Index(transcript.String()[matchedUntil:], text) + len(text)
			}
			send := func(text string) {
				t.Helper()
				// The prompt is printed before keyboard.Listen switches to raw mode.
				require.Eventually(t, func() bool {
					state, err := term.GetState(int(slave.Fd()))

					return err == nil && !reflect.DeepEqual(state, initialState)
				}, 5*time.Second, time.Millisecond, "terminal must be ready for input")
				_, err := terminal.WriteString(text)
				require.NoError(t, err)
			}
			expect("How do you want to create this transaction?")
			send("\r")
			for _, input := range []struct{ prompt, value string }{
				{"Source account", "world"}, {"Destination account", "alice"}, {"Amount", tc.amount},
			} {
				expect(input.prompt)
				send(input.value)
				expect(input.prompt + ": " + input.value)
				send("\r")
			}
			expect("posting input rejected")
			for {
				select {
				case <-childDone:
					require.NoError(t, childError, transcript.String())
					require.Zero(t, fixture.calls.Load(), "interactive invalid input must never reach Apply")

					return
				case b, ok := <-output:
					if !ok {
						output = nil

						continue
					}
					transcript.WriteByte(b)
				case <-ctx.Done():
					t.Fatalf("waiting for command exit: %v\n%s", ctx.Err(), transcript.String())
				}
			}
		})
	}
}
