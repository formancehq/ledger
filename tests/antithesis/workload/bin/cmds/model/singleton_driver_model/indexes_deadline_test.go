package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestReconcileIndexes_BlockedNodeHasIndependentDeadline(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	bulk := oracle.Bulk{Requests: []*servicepb.Request{oracletest.CreateIndexReq(assetIndexID())}}
	result := c.modelState.Apply(bulk)
	require.True(t, result.OK)
	c.modelState = result.State
	c.recordIndexCreates(bulk, &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: 10}}})
	c.modelState.SetIndexActive("L", assetIndexCanonical)
	ready := &servicepb.GetIndexStatusResponse{
		LastIndexedSequence: 10,
		Indexes: []*servicepb.IndexEntry{{
			Ledger: "L", Index: &commonpb.Index{Id: assetIndexID()}, CurrentVersion: 1,
		}},
	}
	blocked, blockedNode := newIndexPollDeadlineNode(t, ready, true)
	healthy, healthyNode := newIndexPollDeadlineNode(t, ready, false)
	conns := internal.PerNodeConns{blockedNode, healthyNode}

	// The first real RPC waits for its request context to end. Its deadline
	// must release the poller while the longer driver context remains live.
	ctx, cancel := context.WithTimeout(t.Context(), 4*indexPollInterval)
	defer cancel()
	reconcileIndexes(ctx, c, conns)
	require.NoError(t, ctx.Err(), "a blocked node must not exhaust the parent deadline")
	_, active := c.modelState.Ledger("L").IndexState(assetIndexCanonical)
	require.False(t, active, "the timed-out node must demote the index")
	for _, node := range []*indexPollDeadlineServer{blocked, healthy} {
		observed := node.observations()
		require.Len(t, observed, 1, "the same poll must sample the healthy node after the blocked node")
		require.True(t, observed[0].hasDeadline)
		// gRPC transmits a relative timeout, then reconstructs the server
		// deadline. Allow encoding/transport slack, not the parent's lifetime.
		require.LessOrEqual(t, observed[0].remaining, indexPollInterval+100*time.Millisecond)
		require.Equal(t, []string{"stale"}, observed[0].consistency)
	}

	// The failed node answers the next request: readiness must recover, with
	// exactly one additional RPC per node and the same live parent context.
	reconcileIndexes(ctx, c, conns)
	require.NoError(t, ctx.Err())
	_, active = c.modelState.Ledger("L").IndexState(assetIndexCanonical)
	require.True(t, active, "a fail-then-success poll must restore active readiness")
	for _, node := range []*indexPollDeadlineServer{blocked, healthy} {
		require.Len(t, node.observations(), 2)
	}
}

type indexPollDeadlineObservation struct {
	hasDeadline bool
	remaining   time.Duration
	consistency []string
}

// A real gRPC server exercises the generated client and transmitted deadline
// and consistency metadata. Only the first request to the blocked node stalls.
type indexPollDeadlineServer struct {
	servicepb.UnimplementedBucketServiceServer
	response   *servicepb.GetIndexStatusResponse
	blockFirst bool
	mu         sync.Mutex
	seen       []indexPollDeadlineObservation
}

func (s *indexPollDeadlineServer) observations() []indexPollDeadlineObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]indexPollDeadlineObservation(nil), s.seen...)
}

func (s *indexPollDeadlineServer) GetIndexStatus(ctx context.Context, _ *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
	deadline, hasDeadline := ctx.Deadline()
	md, _ := metadata.FromIncomingContext(ctx)
	observed := indexPollDeadlineObservation{hasDeadline: hasDeadline, consistency: md.Get("x-consistency")}
	if hasDeadline {
		observed.remaining = time.Until(deadline)
	}
	s.mu.Lock()
	block := s.blockFirst && len(s.seen) == 0
	s.seen = append(s.seen, observed)
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return s.response, nil
}

func newIndexPollDeadlineNode(t *testing.T, response *servicepb.GetIndexStatusResponse, blockFirst bool) (*indexPollDeadlineServer, *internal.PerNodeConn) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	handler := &indexPollDeadlineServer{response: response, blockFirst: blockFirst}
	servicepb.RegisterBucketServiceServer(server, handler)
	go func() { _ = server.Serve(listener) /* Stop terminates Serve. */ }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///index-poll-deadline",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return handler, &internal.PerNodeConn{Bucket: servicepb.NewBucketServiceClient(conn)}
}
