package internal_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

type terminalApplyServer struct {
	servicepb.UnimplementedBucketServiceServer
	attempts atomic.Int32
	entered  chan struct{}
	err      error
}

func (s *terminalApplyServer) Apply(ctx context.Context, _ *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.attempts.Add(1)
	if s.entered != nil {
		s.entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, s.err
}

func factoryForwardingClient(t *testing.T, server servicepb.BucketServiceServer) servicepb.BucketServiceClient {
	t.Helper()
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, pool.AddPeer(1, serveApplyProxy(t, server)))
	forwarder := &forwardingApplyServer{pool: pool, interrupted: make(chan error, 64)}
	conn, err := workloadTestConn(t, serveApplyProxy(t, forwarder), "default")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return servicepb.NewBucketServiceClient(conn)
}

func TestNewGRPCConn_CallerCancellationIsPreserved(t *testing.T) {
	server := &terminalApplyServer{entered: make(chan struct{}, 1)}
	client := factoryForwardingClient(t, server)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	finished := make(chan error, 1)
	go func() {
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{})
		finished <- err
	}()
	select {
	case <-server.entered:
	case <-ctx.Done():
		t.Fatal("RPC did not reach the peer", ctx.Err())
	}
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-finished))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, int32(1), server.attempts.Load())
}

func TestNewGRPCConn_ServerStatusesAreNotRetried(t *testing.T) {
	closeMessage := "grpc: the client connection is closing"
	structured := func(code codes.Code, message, reason string) *status.Status {
		st, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
			Domain: "ledger", Reason: reason, Metadata: map[string]string{"request": "original"},
		})
		require.NoError(t, err)
		return st
	}
	for _, test := range []struct {
		name string
		st   *status.Status
	}{
		{"remote canceled", status.New(codes.Canceled, "remote operation canceled")},
		{"remote close lookalike", status.New(codes.Canceled, closeMessage)},
		{"unknown close lookalike", status.New(codes.Unknown, closeMessage)},
		{"structured canceled", structured(codes.Canceled, closeMessage, "FUTURE_REASON")},
		{"business rejection", structured(codes.FailedPrecondition, "already reverted", domain.ErrReasonTransactionAlreadyReverted)},
		{"immediate maintenance", structured(codes.Unavailable, "maintenance", domain.ErrReasonMaintenanceMode)},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &terminalApplyServer{err: test.st.Err()}
			client := factoryForwardingClient(t, server)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{})
			require.NoError(t, ctx.Err())
			require.True(t, proto.Equal(test.st.Proto(), status.Convert(err).Proto()))
			require.Equal(t, int32(1), server.attempts.Load())
			require.False(t, internal.IsMaintenanceAfterAmbiguousCommit(err))
		})
	}
}
