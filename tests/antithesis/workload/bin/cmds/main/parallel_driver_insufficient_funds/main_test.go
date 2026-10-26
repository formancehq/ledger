package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
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
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The proxy loses only the funding response after the real server commits it.
// The workload client retries that same request through its production retry interceptor.
type lostFundingResponse struct {
	servicepb.UnimplementedBucketServiceServer

	backend              servicepb.BucketServiceClient
	mu                   sync.Mutex
	fundAttempts         []*servicepb.ApplyRequest
	keys                 []string
	probeErr             error
	lost                 bool
	drainBalance         string
	drainReadConsistency string
}

func (s *lostFundingResponse) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	key := req.GetUnsigned().GetIdempotencyKey()
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	if key == "insufficient-funds-insuf-test-fund" {
		s.mu.Lock()
		s.fundAttempts = append(s.fundAttempts, proto.Clone(req).(*servicepb.ApplyRequest))
		s.mu.Unlock()
	}
	resp, err := s.backend.Apply(ctx, req)
	if key == "insufficient-funds-insuf-test-probe" {
		s.mu.Lock()
		s.probeErr = err
		s.mu.Unlock()
	}
	if err != nil {
		return nil, err
	}
	if key == "insufficient-funds-insuf-test-fund" {
		s.mu.Lock()
		if !s.lost {
			s.lost = true
			s.mu.Unlock()

			return nil, status.Error(codes.Unavailable, "committed funding response lost")
		}
		s.mu.Unlock()
	}

	return resp, nil
}

func (s *lostFundingResponse) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	return s.backend.GetLedger(ctx, req)
}

func (s *lostFundingResponse) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.backend.GetAccount(ctx, req)
	if err == nil {
		vol := resp.FindVolume("USD/2", "")
		s.mu.Lock()
		if vol != nil && vol.GetBalance() != nil {
			s.drainBalance = vol.GetBalance().DecimalString()
		}
		s.drainReadConsistency = fmt.Sprint(md.Get("x-consistency"))
		s.mu.Unlock()
	}

	return resp, err
}

func TestLostFundingResponseDoesNotProduceFalseOverdraft(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "insufficient-funds-lost-response", Ports: lease.Ports(),
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
	backendConn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backendConn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(backendConn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})

		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	backend := servicepb.NewBucketServiceClient(backendConn)
	testserver.WaitForWriteAdmission(t, ctx, backend)

	proxy := &lostFundingResponse{backend: backend}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(grpcServer, proxy)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	t.Setenv("LEDGER_GRPC_ADDR", listener.Addr().String())
	t.Setenv("LEDGER_NO_RETRY", "")
	t.Setenv("LEDGER_RETRY_FOREVER", "")
	conn, err := internal.NewGRPCConn()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	runInsufficientFunds(ctx, servicepb.NewBucketServiceClient(conn), "insuf-test", "insuf-users:2", 8066)
	require.NoError(t, ctx.Err())

	proxy.mu.Lock()
	lost, attempts, keys := proxy.lost, append([]*servicepb.ApplyRequest(nil), proxy.fundAttempts...), append([]string(nil), proxy.keys...)
	drainBalance, consistency, probeErr := proxy.drainBalance, proxy.drainReadConsistency, proxy.probeErr
	proxy.mu.Unlock()
	require.True(t, lost)
	require.Len(t, attempts, 2)
	require.True(t, proto.Equal(attempts[0], attempts[1]))
	require.Len(t, keys, 6)
	require.Equal(t, []string{
		"insufficient-funds-insuf-test-fund",
		"insufficient-funds-insuf-test-fund",
		"insufficient-funds-insuf-test-drain",
		"insufficient-funds-insuf-test-probe",
		"insufficient-funds-insuf-test-force",
	}, keys[1:]) // The creation helper supplies its own distinct key.
	require.NotEmpty(t, keys[0])
	require.Equal(t, "0", drainBalance)
	require.Equal(t, "[linearizable]", consistency)
	require.True(t, internal.HasErrorReason(probeErr, domain.ErrReasonInsufficientFunds))

	transactions, err := actions.ListAllTransactions(ctx, backend, "insuf-test")
	require.NoError(t, err)
	require.Len(t, transactions, 3, "one funding, one drain, one forced overdraft; rejected probe creates none")
	account, err := backend.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: "insuf-test", Address: "insuf-users:2"})
	require.NoError(t, err)
	require.Equal(t, "-1", account.FindVolume("USD/2", "").GetBalance().DecimalString())
}
