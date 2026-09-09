package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

// Service forwarding uses this pool for both Bucket and Cluster RPCs. Its
// protocol credentials must coexist with the cluster-secret credentials.
func TestConnectionPoolAdvertisesProtocolWithBearerToken(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		headers := metadata.MD{
			grpcprotocol.MetadataKey: md.Get(grpcprotocol.MetadataKey),
			"authorization":          md.Get("authorization"),
		}
		if err := grpc.SendHeader(ctx, headers); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
	t.Cleanup(server.Stop)

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{AuthToken: "test-cluster-secret"})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, pool.AddPeer(1, listener.Addr().String()))
	conn := pool.GetConnection(1)
	require.NotNil(t, conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var headers metadata.MD
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Header(&headers))
	require.NoError(t, err)
	require.Equal(t, []string{grpcprotocol.Version}, headers.Get(grpcprotocol.MetadataKey))
	require.Equal(t, []string{"Bearer test-cluster-secret"}, headers.Get("authorization"))
}
