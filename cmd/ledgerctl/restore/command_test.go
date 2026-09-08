package restore

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

// Restore has its own connection constructor and must not depend on a
// BucketService Discovery preflight: restore-mode servers do not expose it.
func TestRestoreClientAdvertisesProtocol(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if err := grpc.SendHeader(ctx, metadata.MD{grpcprotocol.MetadataKey: md.Get(grpcprotocol.MetadataKey)}); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
	t.Cleanup(server.Stop)

	cmd := &cobra.Command{Use: "restore"}
	cmd.Flags().String("server", listener.Addr().String(), "")
	cmd.Flags().Bool("insecure", true, "")
	cmd.Flags().String("tls-ca-cert", "", "")
	cmd.Flags().String("tls-server-name", "", "")
	_, conn, err := getRestoreClient(cmd)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var headers metadata.MD
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Header(&headers))
	require.NoError(t, err)
	require.Equal(t, []string{grpcprotocol.Version}, headers.Get(grpcprotocol.MetadataKey))
}
