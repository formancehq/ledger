package grpcerr

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// A real handler keeps the RPC in flight until the owning peer connection is
// removed. No authored Canceled status substitutes for grpc-go's close path.
type interruptedUnaryServer struct {
	servicepb.UnimplementedBucketServiceServer

	entered       chan struct{}
	attempts      atomic.Int32
	blockAttempts int32
}

func (s *interruptedUnaryServer) wait(ctx context.Context) error {
	if s.attempts.Add(1) > s.blockAttempts {
		return nil
	}
	s.entered <- struct{}{}
	<-ctx.Done()

	return ctx.Err()
}

func (s *interruptedUnaryServer) Apply(ctx context.Context, _ *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	return &servicepb.ApplyResponse{}, s.wait(ctx)
}

func (s *interruptedUnaryServer) GetIndexStatus(ctx context.Context, _ *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
	return &servicepb.GetIndexStatusResponse{}, s.wait(ctx)
}

func TestConn_PeerConnectionCloseIsNotCallerCancellation(t *testing.T) {
	t.Parallel()
	for _, method := range []string{servicepb.BucketService_Apply_FullMethodName, servicepb.BucketService_GetIndexStatus_FullMethodName} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			handler := &interruptedUnaryServer{entered: make(chan struct{}, 2), blockAttempts: 2}
			servicepb.RegisterBucketServiceServer(server, handler)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)

			pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
			t.Cleanup(func() { require.NoError(t, pool.Close()) })
			require.NoError(t, pool.AddPeer(1, listener.Addr().String()))
			conn := pool.GetConnection(1)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)

			invoke := func(cc grpc.ClientConnInterface, result chan<- error) {
				client := servicepb.NewBucketServiceClient(cc)
				var callErr error
				if method == servicepb.BucketService_Apply_FullMethodName {
					_, callErr = client.Apply(ctx, &servicepb.ApplyRequest{})
				} else {
					_, callErr = client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{})
				}
				result <- callErr
			}
			raw, forwarded := make(chan error, 1), make(chan error, 1)
			go invoke(conn, raw)
			go invoke(NewConn(conn), forwarded)
			for range 2 {
				select {
				case <-handler.entered:
				case <-ctx.Done():
					t.Fatal("RPC did not reach the peer handler", ctx.Err())
				}
			}
			require.NoError(t, pool.RemovePeer(1))
			rawErr, forwardedErr := <-raw, <-forwarded
			require.ErrorIs(t, rawErr, status.Error(codes.Canceled, "grpc: the client connection is closing"))
			require.NoError(t, ctx.Err(), "the caller did not cancel the request")
			t.Logf("raw=%v; forwarded=%v; caller=%v", rawErr, forwardedErr, ctx.Err())
			require.Equal(t, codes.Unavailable, status.Code(forwardedErr), "a closed peer connection is an interrupted request, not caller cancellation")
			require.Equal(t, "grpc: the client connection is closing", status.Convert(forwardedErr).Message())

			// A replacement really serves the next attempt; returning a retryable
			// code from an indefinitely broken fixture would not prove recovery.
			require.NoError(t, pool.AddPeer(1, listener.Addr().String()))
			replacement := pool.GetConnection(1)
			require.NotSame(t, conn, replacement)
			invoke(NewConn(replacement), forwarded)
			require.NoError(t, <-forwarded)
			require.Equal(t, int32(3), handler.attempts.Load())
			require.NoError(t, ctx.Err())
		})
	}
}

func TestConn_UnaryCallerCancellationIsPreserved(t *testing.T) {
	t.Parallel()
	handler := &interruptedUnaryServer{entered: make(chan struct{}, 1), blockAttempts: 1}
	client := dialWrapped(t, handler)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{})
		result <- err
	}()
	select {
	case <-handler.entered:
	case <-ctx.Done():
		t.Fatal("RPC did not reach the peer handler", ctx.Err())
	}
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-result))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, int32(1), handler.attempts.Load())
}

func TestConn_UnaryServerStatusesArePreserved(t *testing.T) {
	t.Parallel()
	closeMessage := "grpc: the client connection is closing"
	t.Run("peer-authored close status on a live connection", func(t *testing.T) {
		client := dialWrapped(t, &rejectingServer{err: status.Error(codes.Canceled, closeMessage)})
		_, err := client.GetTransaction(t.Context(), &servicepb.GetTransactionRequest{})
		require.ErrorIs(t, err, status.Error(codes.Canceled, closeMessage), "status equality alone does not establish local transport closure")
	})
	for _, code := range []codes.Code{codes.Canceled, codes.Unknown, codes.Internal, codes.Aborted, codes.FailedPrecondition} {
		t.Run(code.String(), func(t *testing.T) {
			client := dialWrapped(t, &rejectingServer{err: status.Error(code, "server operation failed")})
			_, err := client.GetTransaction(t.Context(), &servicepb.GetTransactionRequest{})
			require.Equal(t, code, status.Code(err))
			require.Equal(t, "server operation failed", status.Convert(err).Message())
		})
	}
	for _, domain := range []string{"ledger", "another-service"} {
		t.Run(domain, func(t *testing.T) {
			upstream, err := status.New(codes.Canceled, closeMessage).WithDetails(
				&errdetails.ErrorInfo{Domain: domain, Reason: "FUTURE_REASON", Metadata: map[string]string{"request": "original"}},
				&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)},
			)
			require.NoError(t, err)
			client := dialWrapped(t, &rejectingServer{err: upstream.Err()})
			_, got := client.GetTransaction(t.Context(), &servicepb.GetTransactionRequest{})
			require.True(t, proto.Equal(upstream.Proto(), status.Convert(got).Proto()), "details must not be mistaken for grpc-go's bare close status")
		})
	}
	t.Run("definitive second revert", func(t *testing.T) {
		upstream := businessStatus(t, codes.FailedPrecondition, "already reverted", domain.ErrReasonTransactionAlreadyReverted)
		client := dialWrapped(t, &rejectingServer{err: upstream})
		_, got := client.GetTransaction(t.Context(), &servicepb.GetTransactionRequest{})
		require.True(t, proto.Equal(status.Convert(upstream).Proto(), status.Convert(got).Proto()))
	})
}

// A client interceptor deterministically closes the real connection after the
// peer's status arrived but before Conn.Invoke observes local state. This pins
// the residual attribution limit; it does not simulate grpc-go's close error.
func TestConn_ServerStatusBeforeLocalShutdown(t *testing.T) {
	t.Parallel()
	closeMessage := "grpc: the client connection is closing"
	structured, err := status.New(codes.Canceled, closeMessage).WithDetails(
		&errdetails.ErrorInfo{Domain: "another-service", Reason: "FUTURE_REASON"},
	)
	require.NoError(t, err)
	for _, test := range []struct {
		name     string
		upstream *status.Status
		want     *status.Status
	}{
		{"bare close lookalike", status.New(codes.Canceled, closeMessage), status.New(codes.Unavailable, closeMessage)},
		{"other cancellation", status.New(codes.Canceled, "remote cancellation"), status.New(codes.Canceled, "remote cancellation")},
		{"unknown lookalike", status.New(codes.Unknown, closeMessage), status.New(codes.Unknown, closeMessage)},
		{"structured lookalike", structured, structured},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := dialWrapped(t, &rejectingServer{err: test.upstream.Err()}, grpc.WithUnaryInterceptor(
				func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
					err := invoker(ctx, method, req, reply, cc, opts...)
					require.True(t, proto.Equal(test.upstream.Proto(), status.Convert(err).Proto()), "the peer's response arrived before local closure")
					require.NotEqual(t, connectivity.Shutdown, cc.GetState())
					require.NoError(t, cc.Close())
					require.Equal(t, connectivity.Shutdown, cc.GetState())
					require.NoError(t, ctx.Err(), "the caller remains live")

					return err
				},
			))
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			_, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{})
			require.True(t, proto.Equal(test.want.Proto(), status.Convert(err).Proto()), "local closure cannot prove the origin of an identical bare close status")
		})
	}
}
