package grpcerr_test

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
)

// The backend is a real single-node Ledger (admission, Raft, FSM and Pebble).
// This proxy only delays delivery of the first acknowledged commit's response.
type lostApplyResponseServer struct {
	servicepb.UnimplementedBucketServiceServer

	client    servicepb.BucketServiceClient
	committed chan *servicepb.ApplyResponse
	requests  chan *servicepb.ApplyRequest
	attempts  atomic.Int32
}

func (s *lostApplyResponseServer) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	attempt := s.attempts.Add(1)
	s.requests <- proto.Clone(req).(*servicepb.ApplyRequest)
	var trailers metadata.MD
	response, err := s.client.Apply(ctx, req, grpc.Trailer(&trailers))
	if err != nil {
		return nil, err
	}
	if attempt == 1 {
		s.committed <- response
		<-ctx.Done()

		return nil, ctx.Err()
	}
	if err := grpc.SetTrailer(ctx, trailers); err != nil {
		return nil, err
	}

	return response, nil
}

// Resolve the current pooled connection on every external attempt, as the
// RoutedController does. The decorator under test is the production boundary.
type forwardingApplyServer struct {
	servicepb.UnimplementedBucketServiceServer

	pool        *transport.ConnectionPool
	interrupted chan error
}

func (s *forwardingApplyServer) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	client := servicepb.NewBucketServiceClient(grpcerr.NewConn(s.pool.GetConnection(1)))
	var trailers metadata.MD
	response, err := client.Apply(ctx, req, grpc.Trailer(&trailers))
	if err != nil {
		s.interrupted <- err

		return nil, err
	}
	if err := grpc.SetTrailer(ctx, trailers); err != nil {
		return nil, err
	}

	return response, nil
}

func serveApplyProxy(t *testing.T, handler servicepb.BucketServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

func TestConn_LostCommittedResponseRetriesWithStableKey(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "peer-close-retry", Ports: lease.Ports(),
		WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard,
	})
	instruments = append(instruments, testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	leaderConn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, leaderConn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(leaderConn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})

		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	leader := servicepb.NewBucketServiceClient(leaderConn)
	_, err = leader.Apply(ctx, actions.WithIdempotencyKey("setup", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)

	loss := &lostApplyResponseServer{client: leader, committed: make(chan *servicepb.ApplyResponse, 1), requests: make(chan *servicepb.ApplyRequest, 2)}
	peerAddr := serveApplyProxy(t, loss)
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, pool.AddPeer(1, peerAddr))
	forwarder := &forwardingApplyServer{pool: pool, interrupted: make(chan error, 1)}
	followerAddr := serveApplyProxy(t, forwarder)
	callerConn, err := grpc.NewClient(followerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(`{"methodConfig":[{"name":[{"service":"ledger.BucketService","method":"Apply"}],"retryPolicy":{"MaxAttempts":2,"InitialBackoff":"0.001s","MaxBackoff":"0.001s","BackoffMultiplier":1,"RetryableStatusCodes":["UNAVAILABLE"]}}]}`))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callerConn.Close()) })
	request := actions.WithIdempotencyKey("one-logical-transaction", actions.CreateTransactionAction("L", []*commonpb.Posting{commonpb.NewPosting("world", "user", "USD", big.NewInt(10))}, nil, nil))
	type result struct {
		response *servicepb.ApplyResponse
		err      error
		trailers metadata.MD
	}
	finished := make(chan result, 1)
	go func() {
		var trailers metadata.MD
		response, err := servicepb.NewBucketServiceClient(callerConn).Apply(ctx, request, grpc.Trailer(&trailers))
		finished <- result{response, err, trailers}
	}()
	var committed *servicepb.ApplyResponse
	select {
	case committed = <-loss.committed:
	case <-ctx.Done():
		t.Fatal("write did not commit", ctx.Err())
	}
	oldConn := pool.GetConnection(1)
	require.NoError(t, pool.RestartConnection(1))
	require.NotSame(t, oldConn, pool.GetConnection(1))
	var outcome result
	select {
	case outcome = <-finished:
	case <-ctx.Done():
		t.Fatal("retry did not finish", ctx.Err())
	}
	require.NoError(t, ctx.Err(), "caller remains live through both attempts")
	require.NoError(t, outcome.err)
	require.Equal(t, int32(2), loss.attempts.Load())
	interrupted := <-forwarder.interrupted
	require.Equal(t, codes.Unavailable, status.Code(interrupted))
	require.Equal(t, "grpc: the client connection is closing", status.Convert(interrupted).Message())
	t.Logf("commit preceded response loss: interruption=%v; attempts=%d", interrupted, loss.attempts.Load())
	require.True(t, proto.Equal(request, <-loss.requests))
	require.True(t, proto.Equal(request, <-loss.requests), "native gRPC retry must retain the original payload and key")
	require.True(t, proto.Equal(committed, outcome.response), "retry returns the original durable outcome")
	require.Equal(t, []string{"true"}, outcome.trailers.Get("ledger-apply-replayed"))
	transactions, err := actions.ListAllTransactions(ctx, leader, "L")
	require.NoError(t, err)
	require.Len(t, transactions, 1, "lost response must not duplicate the committed transaction")
	account, err := leader.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: "L", Address: "user"})
	require.NoError(t, err)
	require.Len(t, account.GetVolumes(), 1)
	require.Equal(t, "10", account.GetVolumes()[0].GetVolumes().GetBalance())
}
