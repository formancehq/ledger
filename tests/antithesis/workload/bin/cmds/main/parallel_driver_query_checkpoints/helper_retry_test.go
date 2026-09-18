package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	workloadinternal "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// This is a transport fault, not an emulation of checkpoint or deduplication
// semantics: every Apply reaches the real admission/FSM/storage path. The first
// successful mutation is observed through the real registry before its response
// is discarded. The workload's normal retry policy then resubmits the request.
type checkpointResponseLossProxy struct {
	servicepb.UnimplementedBucketServiceServer
	backend servicepb.BucketServiceClient
	cluster clusterpb.ClusterServiceClient

	mu            sync.Mutex
	requests      []*servicepb.ApplyRequest
	responses     []*servicepb.ApplyResponse
	errors        []error
	afterFirst    []uint64
	terminalError error
}

func (p *checkpointResponseLossProxy) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, proto.Clone(req).(*servicepb.ApplyRequest))
	if p.terminalError != nil {
		return nil, p.terminalError
	}
	resp, err := p.backend.Apply(ctx, req)
	p.responses = append(p.responses, resp)
	p.errors = append(p.errors, err)
	if len(p.requests) == 1 && err == nil {
		registry, readErr := p.cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
		if readErr != nil {
			return nil, readErr
		}
		for _, checkpoint := range registry.GetCheckpoints() {
			p.afterFirst = append(p.afterFirst, checkpoint.GetCheckpointId())
		}
		return nil, status.Error(codes.Unavailable, "injected response loss after committed checkpoint mutation")
	}
	return resp, err
}

func TestQueryCheckpointLostResponse(t *testing.T) {
	// NewGRPCConn reads environment variables, so these cases must be sequential.
	for _, tc := range []struct {
		name   string
		seed   int
		delete bool
	}{
		{name: "create below capacity"},
		{name: "create fills tenth slot", seed: 9},
		{name: "delete committed checkpoint", seed: 1, delete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, backend, cluster, client, proxy := checkpointRetryServer(t)
			var victim uint64
			for range tc.seed {
				id, _, err := actions.CreateQueryCheckpoint(ctx, backend)
				require.NoError(t, err)
				victim = id
			}

			var id, sequence uint64
			var err error
			if tc.delete {
				err = actions.DeleteQueryCheckpoint(ctx, client, victim)
			} else {
				id, sequence, err = actions.CreateQueryCheckpoint(ctx, client)
			}

			proxy.mu.Lock()
			requests := append([]*servicepb.ApplyRequest(nil), proxy.requests...)
			responses := append([]*servicepb.ApplyResponse(nil), proxy.responses...)
			attemptErrors := append([]error(nil), proxy.errors...)
			afterFirst := append([]uint64(nil), proxy.afterFirst...)
			proxy.mu.Unlock()
			require.Len(t, requests, 2, "one lost response must cause exactly one retry")
			require.NoError(t, attemptErrors[0], "first mutation must really commit")
			require.NotNil(t, responses[0])
			first, second := responses[0], responses[1]
			firstKey := requests[0].GetUnsigned().GetIdempotencyKey()
			secondKey := requests[1].GetUnsigned().GetIdempotencyKey()
			t.Logf("attempts=2 firstCommitted=true firstKey=%q retryKey=%q registryAfterCommit=%v retryError=%v reason=%s", firstKey, secondKey, afterFirst, err, workloadinternal.ErrorReason(err))
			if tc.delete {
				require.NotContains(t, afterFirst, victim, "delete committed before response loss")
				require.Equal(t, victim, first.GetLogs()[0].GetPayload().GetDeletedQueryCheckpoint().GetCheckpointId())
			} else {
				firstID, _, ok := actions.GetCreatedQueryCheckpoint(first)
				require.True(t, ok)
				require.Contains(t, afterFirst, firstID, "create committed before response loss")
				require.Len(t, afterFirst, tc.seed+1)
				t.Logf("firstCheckpoint=%d returnedCheckpoint=%d", firstID, id)
			}
			require.NoError(t, err, "retry must return the committed outcome")
			if !tc.delete {
				firstID, firstSequence, _ := actions.GetCreatedQueryCheckpoint(first)
				require.Equal(t, firstID, id, "retry must not allocate a second checkpoint")
				require.Equal(t, firstSequence, sequence)
			}
			require.True(t, proto.Equal(first, second), "retry must replay original logs and sequences")
			require.NotEmpty(t, firstKey)
			require.Equal(t, firstKey, secondKey)
			registry, listErr := cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			require.NoError(t, listErr)
			var afterRetry []uint64
			for _, checkpoint := range registry.GetCheckpoints() {
				afterRetry = append(afterRetry, checkpoint.GetCheckpointId())
			}
			require.ElementsMatch(t, afterFirst, afterRetry, "retry must not change the live set")
		})
	}
}

func TestQueryCheckpointIdempotencyControls(t *testing.T) {
	ctx, _, cluster, client, proxy := checkpointRetryServer(t)
	first, _, err := actions.CreateQueryCheckpoint(ctx, client)
	require.NoError(t, err)
	second, _, err := actions.CreateQueryCheckpoint(ctx, client)
	require.NoError(t, err)
	require.NotEqual(t, first, second, "independent creates must stay independent")
	require.NoError(t, actions.DeleteQueryCheckpoint(ctx, client, first))
	err = actions.DeleteQueryCheckpoint(ctx, client, first)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.True(t, workloadinternal.HasErrorReason(err, "CHECKPOINT_NOT_FOUND"))
	err = actions.DeleteQueryCheckpoint(ctx, client, 0)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// One checkpoint remains. Fill the real cap and verify that a new operation
	// still reports capacity, rather than replaying a prior creation's success.
	for range 9 {
		_, _, err = actions.CreateQueryCheckpoint(ctx, client)
		require.NoError(t, err)
	}
	_, _, err = actions.CreateQueryCheckpoint(ctx, client)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, workloadinternal.HasErrorReason(err, "CHECKPOINT_LIMIT_REACHED"))
	registry, err := cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
	require.NoError(t, err)
	require.Len(t, registry.GetCheckpoints(), 10)

	proxy.mu.Lock()
	requests := append([]*servicepb.ApplyRequest(nil), proxy.requests...)
	proxy.mu.Unlock()
	require.Len(t, requests, 16, "only the lost first response should add a retry")
	require.Equal(t, requests[0].GetUnsigned().GetIdempotencyKey(), requests[1].GetUnsigned().GetIdempotencyKey())
	keys := make(map[string]bool)
	for _, request := range requests[1:] {
		key := request.GetUnsigned().GetIdempotencyKey()
		require.NotEmpty(t, key)
		require.False(t, keys[key], "separate helper invocations must use distinct keys")
		keys[key] = true
	}

	// Unexpected terminal responses remain visible; the helper must not turn a
	// missing resource or a server failure into a successful no-op.
	for _, code := range []codes.Code{codes.Internal, codes.FailedPrecondition} {
		injected := status.Error(code, "injected unrelated terminal failure")
		proxy.mu.Lock()
		proxy.terminalError = injected
		before := len(proxy.requests)
		proxy.mu.Unlock()
		_, _, err = actions.CreateQueryCheckpoint(ctx, client)
		require.Equal(t, status.Convert(injected).Proto(), status.Convert(err).Proto())
		err = actions.DeleteQueryCheckpoint(ctx, client, second)
		require.Equal(t, status.Convert(injected).Proto(), status.Convert(err).Proto())
		proxy.mu.Lock()
		after := len(proxy.requests)
		proxy.mu.Unlock()
		require.Equal(t, before+2, after, "terminal failures must not be retried")
	}
}

func checkpointRetryServer(t *testing.T) (context.Context, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, servicepb.BucketServiceClient, *checkpointResponseLossProxy) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "checkpoint-retry", Ports: lease.Ports(),
		WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard,
	})
	instruments = append(instruments, testserver.WithBootstrap(), testservice.InstrumentationFunc(func(_ context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--query-checkpoint-limit", "10")
		return nil
	}))
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	backend := servicepb.NewBucketServiceClient(conn)
	proxy := &checkpointResponseLossProxy{backend: backend, cluster: cluster}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(grpcServer, proxy)
	done := make(chan error, 1)
	go func() { done <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		require.NoError(t, <-done)
	})
	t.Setenv("LEDGER_GRPC_ADDR", listener.Addr().String())
	t.Setenv("LEDGER_NO_RETRY", "")
	t.Setenv("LEDGER_RETRY_FOREVER", "")
	workloadConn, err := workloadinternal.NewGRPCConn()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, workloadConn.Close()) })
	return ctx, backend, cluster, servicepb.NewBucketServiceClient(workloadConn), proxy
}
