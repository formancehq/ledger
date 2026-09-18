package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// oracleLifecycleFixture models acknowledged proposals and source logs, not
// oracle decisions. A late write at a final fence changes the actual next
// source fold, and every witness from earlier attempts remains a real ledger.
type oracleLifecycleFixture struct {
	mu sync.Mutex

	index            uint64
	barriers         int
	targetLogs       int
	witnesses        map[string]oracleLifecycleWitness
	members          map[string]uint32
	unavailable      map[string]bool
	replaceMember    bool
	readinessNodeIDs []uint32
	changesRemaining int // -1 changes every final fence.
	incorrectUsage   bool
	staleTargetReads atomic.Uint32
	readinessReads   atomic.Uint32
	membershipReads  atomic.Uint32
	inactiveRequests atomic.Uint32
}

type oracleLifecycleWitness struct {
	id      uint32
	written bool
}

func newOracleLifecycleFixture(changesRemaining int, incorrectUsage bool) *oracleLifecycleFixture {
	return &oracleLifecycleFixture{
		index: 100, targetLogs: 1, witnesses: make(map[string]oracleLifecycleWitness),
		members: map[string]uint32{"replica-1": 1}, unavailable: make(map[string]bool),
		changesRemaining: changesRemaining, incorrectUsage: incorrectUsage,
	}
}

func (f *oracleLifecycleFixture) bucket(addr string) *oracleTestServer {
	return &oracleTestServer{
		barrierFn: func(ctx context.Context, req *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
			f.mu.Lock()
			_, member := f.members[addr]
			unavailable := f.unavailable[addr]
			f.mu.Unlock()
			if !member {
				f.inactiveRequests.Add(1)
				return nil, status.Error(codes.Unavailable, "source candidate is not a cluster member")
			}
			if unavailable {
				return nil, status.Error(codes.Unavailable, "active source candidate is unreachable")
			}
			return f.barrier(ctx, req)
		},
		applyFn: f.apply, listLedgersFn: f.listLedgers,
		listLogsFn: f.listLogs, getLedgerFn: f.getLedger, statsFn: f.stats,
	}
}

func (f *oracleLifecycleFixture) barrier(context.Context, *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.barriers++
	// Each complete attempt uses source-open, source-close, post-witness,
	// then final barriers. Inject a real source transaction only at the last.
	if f.barriers == 4 && f.changesRemaining != 0 {
		f.targetLogs++
		f.index++
		if f.replaceMember {
			f.members["replica-1"] = 9
			f.index++ // The membership replacement is another committed entry.
		}
		if f.changesRemaining > 0 {
			f.changesRemaining--
		}
	}
	f.index++
	return &servicepb.BarrierResponse{CommitIndex: f.index}, nil
}

func (f *oracleLifecycleFixture) apply(_ context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	batch := req.GetUnsigned()
	if len(batch.GetRequests()) != 1 {
		return nil, status.Error(codes.InvalidArgument, "fixture requires one atomic witness request")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	request := batch.GetRequests()[0]
	if create := request.GetCreateLedger(); create != nil {
		if _, exists := f.witnesses[create.GetName()]; exists {
			return nil, status.Error(codes.AlreadyExists, "witness name reused across attempts")
		}
		f.witnesses[create.GetName()] = oracleLifecycleWitness{id: uint32(100 + len(f.witnesses))}
		f.barriers = 0
		f.index++
		return &servicepb.ApplyResponse{}, nil
	}
	apply := request.GetApply()
	witness, exists := f.witnesses[apply.GetLedger()]
	if !exists || apply.GetAction().GetCreateTransaction() == nil {
		return nil, status.Error(codes.InvalidArgument, "unexpected witness proposal")
	}
	if witness.written {
		return nil, status.Error(codes.AlreadyExists, "witness transaction already exists")
	}
	witness.written = true
	f.witnesses[apply.GetLedger()] = witness
	f.index++
	return &servicepb.ApplyResponse{}, nil
}

func (f *oracleLifecycleFixture) listLedgers(_ *servicepb.ListLedgersRequest, stream servicepb.BucketService_ListLedgersServer) error {
	f.mu.Lock()
	ledgers := []*commonpb.LedgerInfo{{Name: "target", Id: 1}}
	for name, witness := range f.witnesses {
		ledgers = append(ledgers, &commonpb.LedgerInfo{Name: name, Id: witness.id})
	}
	f.mu.Unlock()
	sort.Slice(ledgers, func(i, j int) bool { return ledgers[i].GetName() < ledgers[j].GetName() })
	for _, ledger := range ledgers {
		if err := stream.Send(ledger); err != nil {
			return err
		}
	}
	return nil
}

func (f *oracleLifecycleFixture) listLogs(req *servicepb.ListLogsRequest, stream servicepb.BucketService_ListLogsServer) error {
	f.mu.Lock()
	var logs []*commonpb.Log
	if req.GetLedger() == "target" {
		for id := 1; id <= f.targetLogs; id++ {
			logs = append(logs, sourceTestCreatedLog("target", uint64(id), uint64(id), id))
		}
	} else if witness, exists := f.witnesses[req.GetLedger()]; !exists {
		f.mu.Unlock()
		return status.Error(codes.NotFound, "unknown source ledger")
	} else if witness.written {
		logs = append(logs, sourceTestCreatedLog(req.GetLedger(), 1, 1, 1))
	}
	f.mu.Unlock()
	for _, entry := range logs {
		if err := stream.Send(entry); err != nil {
			return err
		}
	}
	return nil
}

func (f *oracleLifecycleFixture) getLedger(_ context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.GetLedger() == "target" {
		return &commonpb.LedgerInfo{Name: "target", Id: 1}, nil
	}
	witness, exists := f.witnesses[req.GetLedger()]
	if !exists {
		return nil, status.Error(codes.NotFound, "unknown ledger")
	}
	return &commonpb.LedgerInfo{Name: req.GetLedger(), Id: witness.id}, nil
}

func (f *oracleLifecycleFixture) stats(ctx context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.GetLedger() == "target" {
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("x-consistency"); len(values) == 1 && values[0] == "stale" {
			f.staleTargetReads.Add(1)
		}
		postings := uint64(f.targetLogs * (f.targetLogs + 1) / 2)
		if f.incorrectUsage {
			postings--
		}
		return &commonpb.LedgerStats{LogCount: uint64(f.targetLogs), TransactionCount: uint64(f.targetLogs), PostingCount: postings}, nil
	}
	witness, exists := f.witnesses[req.GetLedger()]
	if !exists {
		return nil, status.Error(codes.NotFound, "unknown stats ledger")
	}
	if !witness.written {
		return &commonpb.LedgerStats{}, nil
	}
	return &commonpb.LedgerStats{LogCount: 1, TransactionCount: 1, PostingCount: 1, ReferenceCount: 1}, nil
}

func (f *oracleLifecycleFixture) clusterState(_ context.Context, req *clusterpb.GetClusterStateRequest, addr string) (*clusterpb.ClusterState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, exists := f.members[addr]
	if !exists {
		f.inactiveRequests.Add(1)
		return nil, status.Error(codes.Unavailable, "configured pod slot is not a cluster member")
	}
	if req.GetNodeId() != 0 {
		f.readinessReads.Add(1)
		f.readinessNodeIDs = append(f.readinessNodeIDs, req.GetNodeId())
	}
	if f.unavailable[addr] {
		return nil, status.Error(codes.Unavailable, "active replica is unreachable")
	}
	if req.GetNodeId() != 0 && req.GetNodeId() != id {
		return nil, status.Error(codes.NotFound, "readiness used a stale replica ID")
	}
	var members []*clusterpb.NodeInfo
	if req.GetNodeId() == 0 {
		f.membershipReads.Add(1)
		for address, memberID := range f.members {
			members = append(members, &clusterpb.NodeInfo{Id: memberID, ServiceAddress: address})
		}
		sort.Slice(members, func(i, j int) bool { return members[i].GetId() < members[j].GetId() })
	}
	return &clusterpb.ClusterState{
		LocalNode: id, Nodes: members, SyncProgress: &clusterpb.SyncProgress{Status: "normal"},
		RaftStatus: &clusterpb.RaftStatus{LastPersistedIndex: f.index},
	}, nil
}

type oracleLifecycleClusterServer struct {
	clusterpb.UnimplementedClusterServiceServer
	fixture *oracleLifecycleFixture
	addr    string
}

func (s *oracleLifecycleClusterServer) GetClusterState(ctx context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	return s.fixture.clusterState(ctx, req, s.addr)
}

func newOracleLifecycleClients(t *testing.T, fixture *oracleLifecycleFixture) (servicepb.BucketServiceClient, internal.PerNodeConns) {
	t.Helper()
	conn := newOracleTestConn(t, fixture.bucket("replica-1"), &oracleLifecycleClusterServer{fixture: fixture, addr: "replica-1"})
	client := servicepb.NewBucketServiceClient(conn)
	return client, internal.PerNodeConns{&internal.PerNodeConn{
		Addr: "replica-1", NodeID: 1, Bucket: client, Cluster: clusterpb.NewClusterServiceClient(conn),
	}}
}

func newOracleLifecycleFleet(t *testing.T, fixture *oracleLifecycleFixture, candidates int) (servicepb.BucketServiceClient, internal.PerNodeConns) {
	t.Helper()
	var conns internal.PerNodeConns
	for i := 1; i <= candidates; i++ {
		addr := fmt.Sprintf("replica-%d", i)
		conn := newOracleTestConn(t, fixture.bucket(addr), &oracleLifecycleClusterServer{fixture: fixture, addr: addr})
		conns = append(conns, &internal.PerNodeConn{
			// Candidate addresses are not proof of membership or node ID.
			Addr: addr, Bucket: servicepb.NewBucketServiceClient(conn), Cluster: clusterpb.NewClusterServiceClient(conn),
		})
	}
	return conns[0].Bucket, conns
}

func TestRunOracleRetainsQualifiedIncorrectReplica(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(0, true)
	_, replicas := newOracleLifecycleClients(t, fixture)
	var waits atomic.Uint32
	reports := runOracle(sourceTestContext(t), replicas, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute,
		Wait: func(context.Context) error {
			if waits.Add(1) == 3 {
				return context.DeadlineExceeded
			}
			return nil
		},
	})
	require.Len(t, reports, 1, "a qualified bad counter is a verdict, not a reason to discard expectations")
	report := reports[0]
	require.True(t, report.Qualified)
	require.Empty(t, report.Error)
	require.Equal(t, counts{Logs: 1, Postings: 1}, report.Expected["target"].Counts)
	require.Len(t, report.Observations, 1)
	got := report.Observations[0]
	require.False(t, got.Converged)
	require.Equal(t, 3, got.Attempts)
	require.Equal(t, uint64(1), got.WitnessReferences)
	require.Equal(t, counts{Logs: 1}, got.Mismatches["target"])
	require.Contains(t, got.Error, "usage convergence did not complete")
	require.Equal(t, uint32(3), fixture.staleTargetReads.Load())
	require.Equal(t, uint32(1), fixture.readinessReads.Load())
}

func TestRunOracleRequalifiesAfterFinalSourceChange(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(1, false)
	_, replicas := newOracleLifecycleClients(t, fixture)
	var retries atomic.Uint32
	reports := runOracle(sourceTestContext(t), replicas, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute,
		Wait: func(context.Context) error { retries.Add(1); return nil },
	})
	require.Len(t, reports, 2)
	first, second := reports[0], reports[1]
	require.False(t, first.Qualified)
	require.Contains(t, first.Error, "final source fence")
	require.Contains(t, first.Error, "source horizon changed")
	require.Len(t, first.Observations, 1)
	require.True(t, first.Observations[0].Converged, "a matching sample cannot bypass its final fence")
	require.Equal(t, counts{Logs: 1, Postings: 1}, first.Expected["target"].Counts)
	require.True(t, second.Qualified)
	require.Empty(t, second.Error)
	require.NotEqual(t, first.Witness, second.Witness)
	require.Greater(t, second.Horizon, first.Horizon)
	require.Equal(t, counts{Logs: 2, Postings: 3}, second.Expected["target"].Counts, "retry must fold the late transaction")
	require.Equal(t, counts{Logs: 1, Postings: 1}, second.Expected[first.Witness].Counts, "retry must discover the previous witness as an ordinary source ledger")
	require.Len(t, second.Observations, 1)
	require.True(t, second.Observations[0].Converged)
	require.Equal(t, uint32(1), retries.Load())
	require.Equal(t, uint32(2), fixture.readinessReads.Load())
}

func TestRunOracleReportsUnqualifiedAfterSourceChangeBudget(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(-1, false)
	_, replicas := newOracleLifecycleClients(t, fixture)
	reports := runOracle(sourceTestContext(t), replicas, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute, Wait: func(context.Context) error { return nil },
	})
	require.Len(t, reports, 3)
	for i, report := range reports {
		require.False(t, report.Qualified, "attempt %d must remain unqualified", i+1)
		require.Contains(t, report.Error, "final source fence")
		require.Contains(t, report.Error, "source horizon changed")
		require.Len(t, report.Observations, 1)
		require.True(t, report.Observations[0].Converged)
		logCount := uint64(i + 1)
		require.Equal(t, counts{Logs: logCount, Postings: logCount * (logCount + 1) / 2}, report.Expected["target"].Counts)
		require.NotEmpty(t, report.Witness)
	}
	require.False(t, reports[len(reports)-1].Qualified, "the public liveness verdict must receive an explicit failure report")
	require.Equal(t, uint32(3), fixture.readinessReads.Load())
}

func TestRunOracleCancellationPreservesReplicaEvidenceWithoutFinalFence(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(0, true)
	_, replicas := newOracleLifecycleClients(t, fixture)
	ctx, cancel := context.WithCancel(sourceTestContext(t))
	defer cancel()
	var waits atomic.Uint32
	reports := runOracle(ctx, replicas, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute,
		Wait: func(context.Context) error {
			waits.Add(1)
			cancel() // The first bad sample exists before the whole command expires.
			return ctx.Err()
		},
	})
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Len(t, reports, 1)
	report := reports[0]
	require.False(t, report.Qualified)
	require.Contains(t, report.Error, "final source fence")
	require.Contains(t, report.Error, "canceled")
	require.NotZero(t, report.Horizon)
	require.Equal(t, counts{Logs: 1, Postings: 1}, report.Expected["target"].Counts)
	require.Len(t, report.Observations, 1)
	got := report.Observations[0]
	require.False(t, got.Converged)
	require.Equal(t, 1, got.Attempts)
	require.Equal(t, uint64(1), got.WitnessReferences)
	require.Equal(t, counts{Logs: 1}, got.Mismatches["target"])
	require.Contains(t, got.Error, "usage convergence did not complete")
	require.Contains(t, got.Error, fmt.Sprint(context.Canceled))
	require.Equal(t, uint32(1), waits.Load())
	require.Equal(t, uint32(1), fixture.staleTargetReads.Load())
}

func TestRunOracleRequiresOnlyCurrentMembersFromSevenCandidates(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(0, false)
	fixture.members = map[string]uint32{"replica-1": 1, "replica-2": 2, "replica-3": 3}
	_, candidates := newOracleLifecycleFleet(t, fixture, 7)
	// Interleave absent slots so result identities cannot accidentally come
	// from the original candidate slice's positions.
	candidates = internal.PerNodeConns{candidates[0], candidates[6], candidates[5], candidates[2], candidates[4], candidates[1], candidates[3]}
	reports := runOracle(sourceTestContext(t), candidates, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute,
		Wait: func(context.Context) error { return context.DeadlineExceeded },
	})
	require.Len(t, reports, 1)
	report := reports[0]
	require.True(t, report.Qualified)
	require.Empty(t, report.Error)
	require.Len(t, report.Observations, 3, "four unused pod slots must not become failing replicas")
	for i, got := range report.Observations {
		require.True(t, got.Converged, "%+v", got)
		require.Equal(t, uint32(i+1), got.NodeID)
		require.Equal(t, fmt.Sprintf("replica-%d", i+1), got.Node)
	}
	require.Equal(t, uint32(1), fixture.membershipReads.Load())
	require.Equal(t, uint32(3), fixture.readinessReads.Load())
	require.Equal(t, uint32(3), fixture.staleTargetReads.Load())
	require.Zero(t, fixture.inactiveRequests.Load(), "do not poll absent pod slots")
	for _, candidate := range candidates {
		require.Zero(t, candidate.NodeID, "membership resolution must not mutate the reusable candidates")
	}
}

func TestRunOracleRetainsUnavailableActiveMember(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(0, false)
	fixture.members = map[string]uint32{"replica-1": 1, "replica-2": 2, "replica-3": 3}
	fixture.unavailable["replica-3"] = true
	_, candidates := newOracleLifecycleFleet(t, fixture, 7)
	reports := runOracle(sourceTestContext(t), candidates, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute,
		Wait: func(context.Context) error { return context.DeadlineExceeded },
	})
	require.Len(t, reports, 1)
	report := reports[0]
	require.True(t, report.Qualified)
	require.Len(t, report.Observations, 3, "unreachable membership is required, unlike an unused candidate slot")
	var converged, failed int
	for _, got := range report.Observations {
		if got.Converged {
			converged++
			continue
		}
		failed++
		require.Equal(t, uint32(3), got.NodeID)
		require.Equal(t, "replica-3", got.Node)
		require.Contains(t, got.Error, "replica did not reach source horizon")
		require.Contains(t, got.Error, "active replica is unreachable")
	}
	require.Equal(t, 2, converged)
	require.Equal(t, 1, failed)
	require.Equal(t, uint32(3), fixture.readinessReads.Load())
	require.Zero(t, fixture.inactiveRequests.Load())
}

func TestRunOracleRefreshesMemberIDsAfterSourceInvalidation(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(1, false)
	fixture.replaceMember = true
	_, candidates := newOracleLifecycleClients(t, fixture)
	reports := runOracle(sourceTestContext(t), candidates, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute, Wait: func(context.Context) error { return nil },
	})
	require.Len(t, reports, 2)
	require.False(t, reports[0].Qualified)
	require.Contains(t, reports[0].Error, "source horizon changed")
	require.True(t, reports[1].Qualified)
	require.Len(t, reports[1].Observations, 1)
	require.True(t, reports[1].Observations[0].Converged, "second observation must use replacement member ID 9")
	require.Equal(t, uint32(9), reports[1].Observations[0].NodeID)
	require.Equal(t, uint32(2), fixture.membershipReads.Load())
	fixture.mu.Lock()
	readIDs := append([]uint32(nil), fixture.readinessNodeIDs...)
	fixture.mu.Unlock()
	require.Equal(t, []uint32{1, 9}, readIDs)
	require.Equal(t, uint32(1), candidates[0].NodeID, "fresh IDs belong to the qualified attempt, not stale candidate state")
}

func TestRunOracleSelectsReachableSourceAfterAbsentFirstCandidate(t *testing.T) {
	t.Parallel()
	fixture := newOracleLifecycleFixture(0, false)
	fixture.members = map[string]uint32{"replica-1": 1, "replica-2": 2, "replica-3": 3}
	_, candidates := newOracleLifecycleFleet(t, fixture, 7)
	// Slot 7 does not exist in the actual three-member cluster. Its Bucket
	// Barrier and Cluster calls fail over the wire, rather than merely leaving
	// its startup NodeID unset.
	candidates = append(internal.PerNodeConns{candidates[6]}, candidates[:6]...)
	reports := runOracle(sourceTestContext(t), candidates, oracleConfig{
		Attempts: 3, ConvergenceWindow: time.Minute,
		Wait: func(context.Context) error { return context.DeadlineExceeded },
	})
	require.Len(t, reports, 1, "source selection should try another candidate in the same attempt")
	report := reports[0]
	require.True(t, report.Qualified)
	require.Empty(t, report.Error)
	require.Equal(t, counts{Logs: 1, Postings: 1}, report.Expected["target"].Counts)
	require.Len(t, report.Observations, 3)
	for i, got := range report.Observations {
		require.True(t, got.Converged, "%+v", got)
		require.Equal(t, uint32(i+1), got.NodeID)
		require.Equal(t, fmt.Sprintf("replica-%d", i+1), got.Node)
	}
	require.Equal(t, uint32(1), fixture.inactiveRequests.Load(), "only the failed selection probe should contact the absent candidate")
	require.Equal(t, uint32(1), fixture.membershipReads.Load(), "membership must use the selected healthy source")
	require.Equal(t, uint32(3), fixture.readinessReads.Load())
}
