package internal_test

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The backend is a real single-node Ledger (admission, Raft, FSM and Pebble).
// This proxy only delays delivery of the first acknowledged commit's response.
type lostApplyResponseServer struct {
	commonpb.UnimplementedBucketServiceServer

	client    commonpb.BucketServiceClient
	committed chan *commonpb.ApplyResponse
	requests  chan *commonpb.ApplyRequest
	attempts  atomic.Int32
}

func (s *lostApplyResponseServer) Apply(ctx context.Context, req *commonpb.ApplyRequest) (*commonpb.ApplyResponse, error) {
	attempt := s.attempts.Add(1)
	s.requests <- proto.Clone(req).(*commonpb.ApplyRequest)
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
	commonpb.UnimplementedBucketServiceServer

	pool        *transport.ConnectionPool
	interrupted chan error
}

func (s *forwardingApplyServer) Apply(ctx context.Context, req *commonpb.ApplyRequest) (*commonpb.ApplyResponse, error) {
	client := commonpb.NewBucketServiceClient(grpcerr.NewConn(s.pool.GetConnection(1)))
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

func serveApplyProxy(t *testing.T, handler commonpb.BucketServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	commonpb.RegisterBucketServiceServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

// Keep the original native retry trigger as a separate control; it is not the
// retry configuration used by NewGRPCConn after EN-1627.
func TestConn_LostCommittedResponseRetriesWithStableKey(t *testing.T) {
	t.Parallel()
	testLostCommittedResponse(t, "native")
}

func TestNewGRPCConn_LostCommittedResponse(t *testing.T) {
	// NewGRPCConn reads process environment: these cases cannot run in parallel.
	for _, mode := range []string{"default", "forever", "disabled", "maintenance"} {
		t.Run(mode, func(t *testing.T) {
			testLostCommittedResponse(t, mode)
		})
	}
}

func workloadTestConn(t *testing.T, addr, mode string) (*grpc.ClientConn, error) {
	t.Helper()
	t.Setenv("LEDGER_GRPC_ADDR", addr)
	t.Setenv("LEDGER_NO_RETRY", "")
	t.Setenv("LEDGER_RETRY_FOREVER", "")
	if mode == "disabled" {
		t.Setenv("LEDGER_NO_RETRY", "1")
	}
	if mode == "forever" {
		t.Setenv("LEDGER_RETRY_FOREVER", "1")
	}

	return internal.NewGRPCConn()
}

func testLostCommittedResponse(t *testing.T, mode string) {
	t.Helper()
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
	cluster := commonpb.NewClusterServiceClient(leaderConn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &commonpb.GetClusterStateRequest{})

		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	leader := commonpb.NewBucketServiceClient(leaderConn)
	testserver.WaitForWriteAdmission(t, ctx, leader)
	_, err = leader.Apply(ctx, actions.WithIdempotencyKey("setup", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)

	loss := &lostApplyResponseServer{client: leader, committed: make(chan *commonpb.ApplyResponse, 1), requests: make(chan *commonpb.ApplyRequest, 4)}
	peerAddr := serveApplyProxy(t, loss)
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, pool.AddPeer(1, peerAddr))
	forwarder := &forwardingApplyServer{pool: pool, interrupted: make(chan error, 4)}
	followerAddr := serveApplyProxy(t, forwarder)
	var callerConn *grpc.ClientConn
	if mode == "native" {
		callerConn, err = grpc.NewClient(followerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(`{"methodConfig":[{"name":[{"service":"ledger.BucketService","method":"Apply"}],"retryPolicy":{"MaxAttempts":2,"InitialBackoff":"0.001s","MaxBackoff":"0.001s","BackoffMultiplier":1,"RetryableStatusCodes":["UNAVAILABLE"]}}]}`))
	} else {
		callerConn, err = workloadTestConn(t, followerAddr, mode)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callerConn.Close()) })
	request := actions.WithIdempotencyKey("one-logical-transaction", actions.CreateTransactionAction("L", []*commonpb.Posting{commonpb.NewPosting("world", "user", "USD", big.NewInt(10))}, nil, nil))
	type result struct {
		response *commonpb.ApplyResponse
		err      error
		trailers metadata.MD
	}
	finished := make(chan result, 1)
	go func() {
		var trailers metadata.MD
		response, err := commonpb.NewBucketServiceClient(callerConn).Apply(ctx, request, grpc.Trailer(&trailers))
		finished <- result{response, err, trailers}
	}()
	var committed *commonpb.ApplyResponse
	select {
	case committed = <-loss.committed:
	case <-ctx.Done():
		t.Fatal("write did not commit", ctx.Err())
	}
	if mode == "maintenance" {
		// Make the real admission gate reject the retry after the transaction
		// committed and before the first response can reach the caller.
		_, err = leader.Apply(ctx, actions.WithIdempotencyKey("enable-maintenance", actions.SetMaintenanceModeAction(true)))
		require.NoError(t, err)
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
	require.NoError(t, ctx.Err(), "caller remains live through recovery")
	interrupted := <-forwarder.interrupted
	require.Equal(t, codes.Unavailable, status.Code(interrupted))
	require.Equal(t, "grpc: the client connection is closing", status.Convert(interrupted).Message())
	require.True(t, internal.IsAmbiguousCommit(interrupted), "the workload must recognize the actual forwarding boundary's close status")
	wantAttempts := int32(2)
	switch mode {
	case "disabled":
		require.Equal(t, int32(1), loss.attempts.Load(), "LEDGER_NO_RETRY must prevent automatic replay")
		require.Equal(t, codes.Unavailable, status.Code(outcome.err))
		// The caller can still explicitly recover the same keyed outcome.
		outcome.response, outcome.err = commonpb.NewBucketServiceClient(callerConn).Apply(ctx, request, grpc.Trailer(&outcome.trailers))
	case "maintenance":
		require.Equal(t, int32(2), loss.attempts.Load(), "maintenance must escape the retry loop")
		require.True(t, internal.HasErrorReason(outcome.err, domain.ErrReasonMaintenanceMode))
		require.True(t, internal.IsMaintenanceAfterAmbiguousCommit(outcome.err), "a real committed write must not become a definitive rejection")
		maintenance := <-forwarder.interrupted
		require.True(t, proto.Equal(status.Convert(maintenance).Proto(), status.Convert(outcome.err).Proto()), "ambiguity wrapper must preserve the structured maintenance status")
		_, err = leader.Apply(ctx, actions.WithIdempotencyKey("disable-maintenance", actions.SetMaintenanceModeAction(false)))
		require.NoError(t, err)
		outcome.response, outcome.err = commonpb.NewBucketServiceClient(callerConn).Apply(ctx, request, grpc.Trailer(&outcome.trailers))
		wantAttempts = 3
	}
	require.NoError(t, outcome.err)
	require.Equal(t, wantAttempts, loss.attempts.Load())
	t.Logf("mode=%s; commit preceded response loss: interruption=%v; attempts=%d", mode, interrupted, loss.attempts.Load())
	for range wantAttempts {
		require.True(t, proto.Equal(request, <-loss.requests), "every attempt must retain the original payload and key")
	}
	require.True(t, proto.Equal(committed, outcome.response), "retry returns the original durable outcome")
	require.Equal(t, []string{"true"}, outcome.trailers.Get("ledger-apply-replayed"))
	transactions, err := actions.ListAllTransactions(ctx, leader, "L")
	require.NoError(t, err)
	require.Len(t, transactions, 1, "lost response must not duplicate the committed transaction")
	account, err := leader.GetAccount(ctx, &commonpb.GetAccountRequest{Ledger: "L", Address: "user"})
	require.NoError(t, err)
	require.Len(t, account.GetVolumes(), 1)
	require.Equal(t, "10", account.GetVolumes()[0].GetVolumes().GetBalance())
}
