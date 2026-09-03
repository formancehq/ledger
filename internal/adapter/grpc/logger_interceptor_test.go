package grpc

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

const (
	unaryLogMarker  = "logger-interceptor-unary-marker"
	streamLogMarker = "logger-interceptor-stream-marker"
)

// contextLoggingHealth logs through logging.FromContext on every RPC so the
// test can prove the request context carries the injected app logger rather
// than degrading to go-libs' bare stderr fallback.
type contextLoggingHealth struct {
	healthpb.UnimplementedHealthServer
}

func (contextLoggingHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	logging.FromContext(ctx).Infof(unaryLogMarker)

	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (contextLoggingHealth) Watch(_ *healthpb.HealthCheckRequest, stream ggrpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	logging.FromContext(stream.Context()).Infof(streamLogMarker)

	return stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING})
}

// syncBuffer is a goroutine-safe bytes.Buffer: the server logs from RPC
// goroutines while the test reads the captured output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// TestNewServiceServer_InjectsLoggerIntoRequestContexts drives the real
// NewServiceServer interceptor chain (not the interceptors in isolation) and
// asserts that a handler resolving logging.FromContext on both a unary and a
// streaming call writes to the configured app logger. Removing
// loggerInterceptor/loggerStreamInterceptor from the chain keeps the RPCs
// succeeding but fails this test, because the lines would land on the
// process stderr instead of the injected writer.
func TestNewServiceServer_InjectsLoggerIntoRequestContexts(t *testing.T) {
	t.Parallel()

	logs := &syncBuffer{}
	logger := logging.NewDefaultLogger(logs, false, false, false)

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := NewServiceServer("", 0, logger, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)

	healthpb.RegisterHealthServer(srv.GetServer(), contextLoggingHealth{})

	require.NoError(t, srv.Listen())

	go func() {
		_ = srv.Serve()
	}()

	t.Cleanup(func() { _ = srv.Stop() })

	conn, err := ggrpc.NewClient(listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	client := healthpb.NewHealthClient(conn)

	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	watch, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	_, err = watch.Recv()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		logged := logs.String()

		return strings.Contains(logged, unaryLogMarker) &&
			strings.Contains(logged, streamLogMarker)
	}, 5*time.Second, 10*time.Millisecond, "handler logs must reach the injected app logger; got: %q", logs.String())
}
