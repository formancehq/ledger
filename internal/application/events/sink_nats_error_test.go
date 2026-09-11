//go:build nats

package events

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNATSSinkConstructorSanitizesMalformedConnectionError(t *testing.T) {
	t.Parallel()
	sink, err := NewNATSSink(NATSSinkConfig{
		URL:    "nats://alice:nats-password@localhost:invalid-port",
		Topic:  "ledger-events",
		Format: FormatProto,
	})
	require.Nil(t, sink)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connecting to NATS")
	require.Contains(t, err.Error(), "invalid port")
	require.NotContains(t, err.Error(), "nats-password")
}

func TestNATSSinkConstructorSanitizesTokenForms(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"nats://token-secret@%s", " nats://token-secret@%s ", "token-secret@%s"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { require.NoError(t, listener.Close()) }()
			done := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- err

					return
				}
				defer func() { _ = conn.Close() }() // Best-effort cleanup of the rejected test peer.
				if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
					done <- err

					return
				}
				if _, err := fmt.Fprint(conn, "INFO {\"server_id\":\"test\",\"version\":\"2.10.0\",\"proto\":1,\"max_payload\":1048576}\r\n"); err != nil {
					done <- err

					return
				}
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						done <- err
						return
					}
					if strings.HasPrefix(line, "CONNECT ") {
						_, err = fmt.Fprint(conn, "-ERR 'rejected token-secret'\r\n")
						done <- err
						return
					}
				}
			}()
			sink, err := NewNATSSink(NATSSinkConfig{URL: fmt.Sprintf(format, listener.Addr()), Topic: "events", Format: FormatProto})
			require.Nil(t, sink)
			require.ErrorContains(t, err, "rejected")
			require.NotContains(t, err.Error(), "token-secret")
			require.NoError(t, <-done)
		})
	}
}
