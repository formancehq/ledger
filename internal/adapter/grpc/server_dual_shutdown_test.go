package grpc

import (
	"crypto/tls"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"
)

func TestDualListenerClosureIsGraceful(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	// Closing before serve is deterministic: cmux closes its child listeners,
	// whose grpc.Serve calls return cmux.ErrServerClosed during normal shutdown.
	require.NoError(t, listener.Close())
	server := &baseServer{
		tlsServer: ggrpc.NewServer(), plaintextServer: ggrpc.NewServer(),
		tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}, name: "shutdown-test",
	}
	require.NoError(t, server.serveDual(listener))
}
