package grpc

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

type retryBoundaryServer struct {
	servicepb.UnimplementedBucketServiceServer

	attempts atomic.Int32
	failure  func(int32) error
}

func (s *retryBoundaryServer) Apply(context.Context, *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	if err := s.failure(s.attempts.Add(1)); err != nil {
		return nil, err
	}

	return &servicepb.ApplyResponse{}, nil
}

func (s *retryBoundaryServer) ListLedgers(*servicepb.ListLedgersRequest, ggrpc.ServerStreamingServer[commonpb.LedgerInfo]) error {
	return s.failure(s.attempts.Add(1))
}

func retryBoundaryClient(t *testing.T, impl *retryBoundaryServer) servicepb.BucketServiceClient {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := ggrpc.NewServer(
		ggrpc.UnaryInterceptor(errorConversionInterceptor(testLogger())),
		ggrpc.StreamInterceptor(errorConversionStreamInterceptor(testLogger())),
	)
	servicepb.RegisterBucketServiceServer(server, impl)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = server.Serve(listener) // Stop closes the listener during cleanup.
	}()
	t.Cleanup(func() {
		server.Stop()
		<-served
	})

	conn, err := ggrpc.NewClient("passthrough:///maintenance-retry",
		ggrpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
		ggrpc.WithDefaultServiceConfig(actions.GRPCRetryPolicy),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return servicepb.NewBucketServiceClient(conn)
}

func TestMaintenanceRejectionStopsNativeRetries(t *testing.T) {
	t.Parallel()

	for _, streaming := range []bool{false, true} {
		for _, forwarded := range []bool{false, true} {
			t.Run(fmt.Sprintf("streaming=%t/forwarded=%t", streaming, forwarded), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					want, err := status.New(codes.Unavailable, "maintenance mode").WithDetails(
						&errdetails.ErrorInfo{Domain: "ledger", Reason: "MAINTENANCE_MODE"},
					)
					require.NoError(t, err)
					var failure = fmt.Errorf("private admission context: %w", domain.ErrMaintenanceMode)
					if forwarded {
						want, err = want.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(7 * time.Second)})
						require.NoError(t, err)
						failure = fmt.Errorf("private routing context: %w", grpcerr.FromStatusError(want.Err()))
					}
					server := &retryBoundaryServer{failure: func(int32) error { return failure }}
					client := retryBoundaryClient(t, server)
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()

					var trailer metadata.MD
					if streaming {
						stream, streamErr := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
						require.NoError(t, streamErr)
						_, err = stream.Recv()
						trailer = stream.Trailer()
					} else {
						_, err = client.Apply(ctx, &servicepb.ApplyRequest{}, ggrpc.Trailer(&trailer))
					}
					require.EqualValues(t, 1, server.attempts.Load(), "maintenance requires operator action, not native retries")
					require.Equal(t, []string{"-1"}, trailer.Get("grpc-retry-pushback-ms"))
					require.True(t, proto.Equal(want.Proto(), status.Convert(err).Proto()), "status and all details must survive: %v", err)
				})
			})
		}
	}
}

func TestNonMaintenanceUnavailableRetriesAndRecovers(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		domain string
		reason string
	}{
		{name: "bare status mentioning maintenance"},
		{name: "foreign reason", domain: "another-service", reason: "MAINTENANCE_MODE"},
		{name: "unknown Ledger reason", domain: "ledger", reason: "FUTURE_UNAVAILABLE"},
		{name: "invalid reason-code pair", domain: "ledger", reason: "LEDGER_DELETED"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				failure := status.New(codes.Unavailable, "maintenance mode")
				if tt.reason != "" {
					var err error
					failure, err = failure.WithDetails(&errdetails.ErrorInfo{Domain: tt.domain, Reason: tt.reason})
					require.NoError(t, err)
				}
				server := &retryBoundaryServer{failure: func(attempt int32) error {
					if attempt == 1 {
						return failure.Err()
					}

					return nil
				}}
				client := retryBoundaryClient(t, server)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				var trailer metadata.MD
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{}, ggrpc.Trailer(&trailer))
				require.NoError(t, err)
				require.EqualValues(t, 2, server.attempts.Load(), "the real policy must recover from a first-attempt failure")
				require.Empty(t, trailer.Get("grpc-retry-pushback-ms"))
			})
		})
	}
}

func TestInvalidMaintenanceReasonDoesNotSetPushback(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		failure, err := status.New(codes.FailedPrecondition, "maintenance mode").WithDetails(
			&errdetails.ErrorInfo{Domain: "ledger", Reason: "MAINTENANCE_MODE"},
		)
		require.NoError(t, err)
		server := &retryBoundaryServer{failure: func(int32) error { return failure.Err() }}
		client := retryBoundaryClient(t, server)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		var trailer metadata.MD
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{}, ggrpc.Trailer(&trailer))
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Empty(t, trailer.Get("grpc-retry-pushback-ms"), "a contradictory status is not a validated maintenance rejection")
	})
}
