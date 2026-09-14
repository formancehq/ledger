package internal

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestNewGRPCConnWithoutRetriesSurfacesMaintenance(t *testing.T) {
	// Environment config belongs to the public constructor, so this test cannot run in parallel.
	t.Setenv("LEDGER_NO_RETRY", "")
	t.Setenv("LEDGER_RETRY_FOREVER", "1")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Setenv("LEDGER_GRPC_ADDR", listener.Addr().String())
	maintenance, err := status.New(codes.Unavailable, "maintenance mode").WithDetails(&errdetails.ErrorInfo{Reason: "MAINTENANCE_MODE"})
	require.NoError(t, err)
	var calls atomic.Int32
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if versions := md.Get(grpcprotocol.MetadataKey); len(versions) != 1 || versions[0] != grpcprotocol.Version {
			return nil, status.Error(codes.FailedPrecondition, "missing supported protocol version")
		}
		if calls.Add(1) == 1 {
			return nil, maintenance.Err()
		}
		return handler(ctx, req)
	}))
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-serveResult) })
	conn, err := NewGRPCConnWithoutRetries()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.True(t, HasErrorReason(err, "MAINTENANCE_MODE"))
	require.EqualValues(t, 1, calls.Load())
	// The ordinary connection still retries a rejected first attempt and succeeds.
	calls.Store(0)
	normal, err := NewGRPCConn()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, normal.Close()) })
	response, err := grpc_health_v1.NewHealthClient(normal).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, response.GetStatus())
	require.EqualValues(t, 2, calls.Load())
}
