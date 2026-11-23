package grpc

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type hangingClusterServer struct {
	clusterpb.UnimplementedClusterServiceServer

	received chan struct{}
}

func (s hangingClusterServer) GetClusterState(ctx context.Context, _ *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	s.received <- struct{}{}
	<-ctx.Done()

	return nil, status.FromContextError(ctx.Err()).Err()
}

type forwardingClusterServer struct {
	clusterpb.UnimplementedClusterServiceServer

	upstream clusterpb.ClusterServiceClient
}

func (s forwardingClusterServer) GetClusterState(ctx context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	return s.upstream.GetClusterState(ctx, req)
}

// stallableConn blocks every write once stalled, like a paused client that
// can no longer send RST_STREAM when its own deadline passes.
type stallableConn struct {
	net.Conn

	stalled <-chan struct{}
	release <-chan struct{}
}

func (c stallableConn) Write(b []byte) (int, error) {
	select {
	case <-c.stalled:
		<-c.release
	default:
	}

	return c.Conn.Write(b)
}

func serveCluster(t *testing.T, impl clusterpb.ClusterServiceServer, wrap func(net.Conn) net.Conn, opts ...ggrpc.ServerOption) clusterpb.ClusterServiceClient {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := ggrpc.NewServer(opts...)
	clusterpb.RegisterClusterServiceServer(server, impl)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = server.Serve(listener) // Stop closes the listener during cleanup.
	}()
	t.Cleanup(func() {
		server.Stop()
		<-served
	})

	conn, err := ggrpc.NewClient("passthrough:///deadline-cancellation",
		ggrpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			conn, err := listener.DialContext(ctx)
			if err != nil {
				return nil, err
			}

			return wrap(conn), nil
		}),
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return clusterpb.NewClusterServiceClient(conn)
}

// grpc-go cancels a handler's context from its own deadline timer, which races
// the context's deadline timer, so a forwarding handler can observe Canceled
// once its incoming deadline has expired. With the caller stalled, that status
// is what reaches it, and it must be DeadlineExceeded.
func TestForwardedCallEndedByIncomingDeadlineIsDeadlineExceeded(t *testing.T) {
	t.Parallel()

	const calls = 200
	received := make(chan struct{}, calls)
	upstream := serveCluster(t, hangingClusterServer{received: received}, func(conn net.Conn) net.Conn { return conn })

	var (
		mu      sync.Mutex
		written = map[codes.Code]int{}
	)
	record := func(ctx context.Context, req any, _ *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		mu.Lock()
		written[status.Code(err)]++
		mu.Unlock()

		return resp, err
	}
	stalled := make(chan struct{})
	release := make(chan struct{})
	proxy := serveCluster(t, forwardingClusterServer{upstream: upstream},
		func(conn net.Conn) net.Conn { return stallableConn{Conn: conn, stalled: stalled, release: release} },
		ggrpc.ChainUnaryInterceptor(record, errorConversionInterceptor(testLogger())),
	)
	t.Cleanup(func() { close(release) })

	var wg sync.WaitGroup
	for range calls {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err := proxy.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
		})
	}
	for range calls {
		<-received
	}
	close(stalled)
	wg.Wait()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return written[codes.DeadlineExceeded]+written[codes.Canceled] == calls
	}, 10*time.Second, 10*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[codes.Code]int{codes.DeadlineExceeded: calls}, written)
}

func TestCanceledStatusConversionFollowsIncomingDeadline(t *testing.T) {
	t.Parallel()

	canceled := status.Error(codes.Canceled, "context canceled")
	for name, tc := range map[string]struct {
		ctx  func(t *testing.T) context.Context
		want codes.Code
	}{
		"no deadline": {
			ctx:  func(t *testing.T) context.Context { return t.Context() },
			want: codes.Canceled,
		},
		"deadline ahead": {
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
				t.Cleanup(cancel)

				return ctx
			},
			want: codes.Canceled,
		},
		"deadline passed": {
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Millisecond))
				t.Cleanup(cancel)

				return ctx
			},
			want: codes.DeadlineExceeded,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := convertToGRPCErrorWithContext(tc.ctx(t), canceled, testLogger())
			require.Equal(t, tc.want, status.Code(got))
			require.Equal(t, "context canceled", status.Convert(got).Message())
		})
	}
}
