package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

// These fixtures serve the generated protocol over bufconn. The cluster index
// advances only on proposals; paginated streams carry actual trailers/errors.
// No oracle verdict is mocked, including SDK failures and invalidated windows.
type identityCluster struct {
	mu            sync.Mutex
	index         uint64
	barriers      int
	members       []member
	scenario      string
	changed       bool
	reads         atomic.Int32
	requestedIDs  []uint64
	streamBlocked chan struct{}
}

type identityServer struct {
	servicepb.UnimplementedBucketServiceServer
	clusterpb.UnimplementedClusterServiceServer

	cluster *identityCluster
	id      uint64
}

func newIdentityCluster(t *testing.T, scenario string) (*identityCluster, internal.PerNodeConns) {
	t.Helper()
	cluster := &identityCluster{index: 100, scenario: scenario, streamBlocked: make(chan struct{}, 1), members: []member{
		{ID: 1, Address: "replica-1", Suffrage: "Voter"},
		{ID: 2, Address: "replica-2", Suffrage: "Voter"},
		{ID: 3, Address: "replica-3", Suffrage: "Learner"},
	}}
	var conns internal.PerNodeConns
	for id := uint64(1); id <= 4; id++ {
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer()
		fixture := &identityServer{cluster: cluster, id: id}
		servicepb.RegisterBucketServiceServer(server, fixture)
		clusterpb.RegisterClusterServiceServer(server, fixture)
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		t.Cleanup(func() {
			server.Stop()
			err := <-done
			require.True(t, err == nil || errors.Is(err, grpc.ErrServerStopped), "serve: %v", err)
		})
		conn, err := grpc.NewClient("passthrough:///identity-fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		// Intentionally stale startup IDs: current membership must resolve them.
		conns = append(conns, &internal.PerNodeConn{Addr: fmt.Sprintf("replica-%d", id), NodeID: 99, Bucket: servicepb.NewBucketServiceClient(conn), Cluster: clusterpb.NewClusterServiceClient(conn)})
	}

	return cluster, conns
}

func (s *identityServer) Barrier(ctx context.Context, _ *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	if s.cluster.scenario == "unavailable_source" && s.id == 1 {
		return nil, status.Error(codes.Unavailable, "first candidate unavailable")
	}
	if s.cluster.scenario == "barrier_deadline" {
		<-ctx.Done()

		return nil, status.FromContextError(ctx.Err()).Err()
	}
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()
	s.cluster.barriers++
	if s.cluster.barriers == 3 && s.cluster.scenario == "late_write" {
		s.cluster.index++ // Actual extra proposal, invalidating the buffered data.
		s.cluster.changed = true
	}
	s.cluster.index++

	return &servicepb.BarrierResponse{CommitIndex: s.cluster.index}, nil
}

func (s *identityServer) GetClusterState(_ context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()
	if req.GetNodeId() != 0 {
		s.cluster.requestedIDs = append(s.cluster.requestedIDs, req.GetNodeId())
		if s.cluster.scenario == "unavailable_member" && s.id == 3 {
			return nil, status.Error(codes.Unavailable, "learner unreachable")
		}
	}
	if req.GetNodeId() == 0 && s.cluster.scenario == "membership_change" && s.cluster.reads.Load() > 0 && !s.cluster.changed {
		// A committed promotion changes the topology. Even a buggy barrier
		// reporting no extra index must not allow this set to qualify.
		s.cluster.members[2].Suffrage = "Voter"
		s.cluster.changed = true
	}
	var nodes []*clusterpb.NodeInfo
	for _, m := range s.cluster.members {
		nodes = append(nodes, &clusterpb.NodeInfo{Id: m.ID, ServiceAddress: m.Address, Suffrage: m.Suffrage})
	}
	localID, index, syncStatus := s.id, s.cluster.index, "normal"
	if s.cluster.scenario == "wrong_status_identity" && s.id == 3 {
		localID = 88
	}
	if s.cluster.scenario == "stale_replica" && s.id == 3 {
		index--
	}
	if s.cluster.scenario == "syncing_replica" && s.id == 3 {
		syncStatus = "snapshotting"
	}

	return &clusterpb.ClusterState{LocalNode: localID, Nodes: nodes, RaftStatus: &clusterpb.RaftStatus{LastPersistedIndex: index}, SyncProgress: &clusterpb.SyncProgress{Status: syncStatus}}, nil
}

func requireLocal(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("x-consistency")
	if len(values) != 1 || values[0] != "stale" {
		return status.Error(codes.FailedPrecondition, "identity read was not explicitly local")
	}

	return nil
}

func (s *identityServer) ListLedgers(_ *servicepb.ListLedgersRequest, stream servicepb.BucketService_ListLedgersServer) error {
	if err := requireLocal(stream.Context()); err != nil {
		return err
	}
	s.cluster.reads.Add(1)
	if s.id == 3 && s.cluster.scenario == "missing_ledger" {
		return nil
	}

	return stream.Send(&commonpb.LedgerInfo{Name: "sentinel", Id: 1})
}

func (s *identityServer) ListAccounts(req *servicepb.ListAccountsRequest, stream servicepb.BucketService_ListAccountsServer) error {
	if err := requireLocal(stream.Context()); err != nil {
		return err
	}
	s.cluster.mu.Lock()
	divergent := s.id == 3 && (s.cluster.scenario == "account_mismatch" || ((s.cluster.scenario == "late_write" || s.cluster.scenario == "membership_change") && !s.cluster.changed))
	s.cluster.mu.Unlock()
	start := 0
	if req.GetOptions().GetCursor() != "" {
		value, err := strconv.Atoi(req.GetOptions().GetCursor())
		if err != nil {
			return status.Error(codes.InvalidArgument, "bad account cursor")
		}
		start = value
	}
	for i := start; i < min(start+2, 3); i++ {
		if s.id == 3 && s.cluster.scenario == "missing_account" && i == 2 {
			continue
		}
		input := uint64(10)
		if divergent && i == 2 {
			input = 20
		}
		account := &commonpb.Account{Address: fmt.Sprintf("raft-sentinel:%d", i), Volumes: []*commonpb.AccountVolume{{Asset: "USD", Color: "BLUE", Volumes: &commonpb.VolumesWithBalance{Input: commonpb.MustBigUintFromDecimal(strconv.FormatUint(input, 10))}}}}
		if err := stream.Send(account); err != nil {
			return err
		}
	}
	if start == 0 {
		stream.SetTrailer(metadata.Pairs("x-next-cursor", "2"))
	}
	if s.id == 3 && s.cluster.scenario == "partial_accounts" {
		return status.Error(codes.Unavailable, "account stream interrupted after rows")
	}

	return nil
}

func (s *identityServer) audit(sequence uint64) *auditpb.AuditEntry {
	hashValue := fmt.Sprintf("hash-%d", sequence)
	if s.id == 3 && s.cluster.scenario == "audit_hash_mismatch" && sequence == 125 {
		hashValue = "divergent-tip"
	}
	if s.id == 3 && s.cluster.scenario == "audit_history_mismatch" && sequence == 75 {
		hashValue = "divergent-history-with-identical-tip"
	}
	version := uint32(1)
	if s.id == 3 && s.cluster.scenario == "audit_version_mismatch" && sequence == 125 {
		version = 2
	}

	entry := &auditpb.AuditEntry{Sequence: sequence, Hash: []byte(hashValue), HashVersion: version}
	if s.cluster.scenario == "audit_body_equal" || s.cluster.scenario == "audit_body_mismatch" {
		entry.OrderCount = 1
	}

	return entry
}

func (s *identityServer) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream servicepb.BucketService_ListAuditEntriesServer) error {
	if err := requireLocal(stream.Context()); err != nil {
		return err
	}
	if req.GetOptions().GetReverse() {
		return stream.Send(s.audit(125))
	}
	s.cluster.mu.Lock()
	partialOnce := s.id == 3 && s.cluster.scenario == "partial_audit_once" && !s.cluster.changed
	if partialOnce {
		s.cluster.changed = true
	}
	s.cluster.mu.Unlock()
	start := uint64(1)
	if req.GetOptions().GetCursor() != "" {
		value, err := strconv.ParseUint(req.GetOptions().GetCursor(), 10, 64)
		if err != nil {
			return status.Error(codes.InvalidArgument, "bad audit cursor")
		}
		start = value
	}
	end := min(start+100, 126)
	for sequence := start; sequence < end; sequence++ {
		if s.id == 3 && s.cluster.scenario == "missing_audit" && sequence == 75 {
			continue
		}
		if err := stream.Send(s.audit(sequence)); err != nil {
			return err
		}
	}
	if s.id == 3 && s.cluster.scenario == "repeated_cursor" {
		stream.SetTrailer(metadata.Pairs("x-next-cursor", "101"))
	} else if end < 126 && (s.id != 3 || s.cluster.scenario != "truncated_audit") {
		cursor := strconv.FormatUint(end, 10)
		stream.SetTrailer(metadata.Pairs("x-next-cursor", cursor))
	}
	if partialOnce || (s.id == 3 && s.cluster.scenario == "partial_audit") {
		return status.Error(codes.Unavailable, "audit stream interrupted after rows")
	}
	if s.cluster.scenario == "blocked_audit" {
		s.cluster.streamBlocked <- struct{}{}
		<-stream.Context().Done()

		return status.FromContextError(stream.Context().Err()).Err()
	}

	return nil
}

func (s *identityServer) GetAuditEntry(ctx context.Context, req *servicepb.GetAuditEntryRequest) (*auditpb.AuditEntry, error) {
	if err := requireLocal(ctx); err != nil {
		return nil, err
	}
	if s.id == 3 && s.cluster.scenario == "missing_tip" {
		return nil, status.Error(codes.NotFound, "tip absent on one replica")
	}

	entry := s.audit(req.GetSequence())
	if s.cluster.scenario == "audit_body_equal" || s.cluster.scenario == "audit_body_mismatch" {
		entry.Items = []*auditpb.AuditItem{{LogSequence: req.GetSequence(), SerializedOrder: []byte("committed-order")}}
		if s.cluster.scenario == "audit_body_mismatch" && s.id == 3 {
			entry.Items[0].SerializedOrder = []byte("different-order")
		}
	}

	return entry, nil
}

func identityConfig() oracleConfig {
	return oracleConfig{Attempts: 2, ConvergenceWindow: 2 * time.Second, CallTimeout: time.Second, Wait: func(ctx context.Context) error { return ctx.Err() }}
}

func TestIdentityOracleCompleteSweep(t *testing.T) {
	t.Parallel()
	cluster, conns := newIdentityCluster(t, "equal")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reports := runOracle(ctx, conns, identityConfig())
	require.Len(t, reports, 1)
	report := reports[0]
	require.True(t, report.Qualified, "%+v", report)
	require.Len(t, report.Observations, 3, "include learner, exclude inactive scale-up slot")
	for _, got := range report.Observations {
		require.Equal(t, uint64(3), got.State.Ledgers["sentinel"].Accounts, "follow account page trailer")
		require.Equal(t, uint64(125), got.State.Audit.Count, "scan beyond first 50 and first 100 entries")
		require.Equal(t, uint64(125), got.State.Audit.Last)
		require.True(t, got.State.Audit.TipPresent)
		require.True(t, got.State.Audit.TailMatches)
		require.Equal(t, report.Observations[0].State, got.State)
	}
	cluster.mu.Lock()
	ids := append([]uint64(nil), cluster.requestedIDs...)
	cluster.mu.Unlock()
	require.NotContains(t, ids, uint64(99), "ignore cached startup identity")
	require.NotContains(t, ids, uint64(4), "do not poll absent member")
	require.Contains(t, ids, uint64(3), "learner must qualify")
}

func TestIdentityOracleRetriesInvalidatedVerdicts(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"late_write", "membership_change"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			_, conns := newIdentityCluster(t, scenario)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			reports := runOracle(ctx, conns, identityConfig())
			require.Len(t, reports, 2)
			require.False(t, reports[0].Qualified)
			require.False(t, equalAccounts(reports[0].Observations[0].State, reports[0].Observations[2].State), "first attempt actually observed a mismatch before its fence")
			require.True(t, reports[1].Qualified, "%+v", reports)
			require.True(t, equalAccounts(reports[1].Observations[0].State, reports[1].Observations[2].State))
		})
	}
}

func TestIdentityOracleRejectsIncompleteEvidence(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"unavailable_member", "wrong_status_identity", "stale_replica", "syncing_replica", "partial_accounts", "partial_audit", "repeated_cursor", "barrier_deadline"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			_, conns := newIdentityCluster(t, scenario)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			config := identityConfig()
			config.Attempts = 1
			if scenario == "barrier_deadline" {
				config.CallTimeout = 50 * time.Millisecond
			}
			reports := runOracle(ctx, conns, config)
			require.Len(t, reports, 1)
			require.False(t, reports[0].Qualified)
			require.NotEmpty(t, reports[0].Error)
			if scenario == "repeated_cursor" {
				require.Contains(t, reports[0].Error, "invalid or repeated cursor")
			}
			if scenario == "unavailable_member" || scenario == "wrong_status_identity" || scenario == "stale_replica" || scenario == "syncing_replica" {
				require.Contains(t, reports[0].Error, "member 3 did not reach exact durable horizon")
			}
		})
	}
}

func TestIdentityOracleRetriesPartialStreamFromTheBeginning(t *testing.T) {
	t.Parallel()
	_, conns := newIdentityCluster(t, "partial_audit_once")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reports := runOracle(ctx, conns, identityConfig())
	require.Len(t, reports, 2)
	require.False(t, reports[0].Qualified)
	require.Contains(t, reports[0].Error, "complete audit stream")
	require.True(t, reports[1].Qualified, "%+v", reports)
	require.Equal(t, uint64(125), reports[1].Observations[2].State.Audit.Count, "do not retain partial rows from the previous attempt")
	require.Equal(t, reports[1].Observations[0].State, reports[1].Observations[2].State, "recovered bytes must match the full reference state")
}

func TestIdentityAuditHonorsCancellationAfterPartialDelivery(t *testing.T) {
	t.Parallel()
	cluster, conns := newIdentityCluster(t, "blocked_audit")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := readAudit(internal.WithStaleConsistency(ctx), conns[2].Bucket)
		done <- err
	}()
	select {
	case <-cluster.streamBlocked:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("fixture never delivered its partial page")
	}
	select {
	case err := <-done:
		require.ErrorContains(t, err, "complete audit stream")
		require.Equal(t, codes.Canceled, status.Code(errors.Unwrap(err)))
	case <-time.After(5 * time.Second):
		t.Fatal("audit scan ignored cancellation")
	}
}

func TestIdentityOracleSelectsAnotherSource(t *testing.T) {
	t.Parallel()
	_, conns := newIdentityCluster(t, "unavailable_source")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reports := runOracle(ctx, conns, identityConfig())
	require.True(t, reports[len(reports)-1].Qualified, "%+v", reports)
	require.Len(t, reports[len(reports)-1].Observations, 3, "selecting another source must not exclude the original member")
}

func TestIdentityOracleSDK(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	t.Parallel()
	for _, scenario := range []string{"equal", "account_mismatch", "missing_account", "missing_ledger", "audit_hash_mismatch", "audit_history_mismatch", "audit_version_mismatch", "missing_audit", "missing_tip", "truncated_audit", "late_write", "membership_change", "partial_audit", "unavailable_member"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			events := sdktest.Capture(t, func() {
				_, conns := newIdentityCluster(t, scenario)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				config := identityConfig()
				publishReports(runOracle(ctx, conns, config))
			})
			if events == nil {
				return
			}
			qualified := scenario != "partial_audit" && scenario != "unavailable_member"
			require.Equal(t, qualified, sdktest.Find(t, events, "cross-node identity oracle obtains a qualified all-member observation").Condition)
			const accounts = "account state is identical across nodes at a common applied index"
			const audit = "audit hash is identical across nodes at the same sequence"
			if !qualified {
				sdktest.Absent(t, events, accounts)
				sdktest.Absent(t, events, audit)
				sdktest.Absent(t, events, "cross-node complete account and audit identity verified")

				return
			}
			accountMismatch := scenario == "account_mismatch" || scenario == "missing_account" || scenario == "missing_ledger"
			auditMismatch := scenario == "audit_hash_mismatch" || scenario == "audit_history_mismatch" || scenario == "audit_version_mismatch" || scenario == "missing_audit" || scenario == "missing_tip" || scenario == "truncated_audit"
			assertEventCondition(t, events, accounts, !accountMismatch)
			assertEventCondition(t, events, audit, !auditMismatch)
			assertEventCondition(t, events, "audit hash version is identical across nodes at the same sequence", scenario != "audit_version_mismatch")
			assertEventCondition(t, events, "complete audit listing and point tip agree at a common applied index", scenario != "missing_tip" && scenario != "truncated_audit")
			const success = "cross-node complete account and audit identity verified"
			if accountMismatch || auditMismatch {
				sdktest.Absent(t, events, success)
			} else {
				require.True(t, sdktest.Find(t, events, success).Condition)
			}
		})
	}
}

func TestIdentityLatestAuditBodySDK(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	t.Parallel()
	for _, scenario := range []string{"audit_body_equal", "audit_body_mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			matching := scenario == "audit_body_equal"
			events := sdktest.Capture(t, func() {
				_, conns := newIdentityCluster(t, scenario)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				// Independently establish that point responses have nonempty
				// bodies, and only Items differ on the last replica. The stored
				// header hash intentionally stays identical across replicas.
				var reference *auditpb.AuditEntry
				for i, conn := range conns[:3] {
					entry, err := conn.Bucket.GetAuditEntry(internal.WithStaleConsistency(ctx), &servicepb.GetAuditEntryRequest{Sequence: 125})
					require.NoError(t, err)
					require.Len(t, entry.GetItems(), 1)
					require.NotEmpty(t, entry.GetItems()[0].GetSerializedOrder())
					if i == 0 {
						reference = entry
					}
					require.Equal(t, matching || i < 2, proto.Equal(reference.GetItems()[0], entry.GetItems()[0]))
					header := proto.Clone(reference).(*auditpb.AuditEntry)
					header.Items = nil
					observedHeader := proto.Clone(entry).(*auditpb.AuditEntry)
					observedHeader.Items = nil
					require.True(t, proto.Equal(header, observedHeader), "body corruption must not alter any header field")
				}
				reports := runOracle(ctx, conns, identityConfig())
				require.Len(t, reports, 1)
				report := reports[0]
				require.True(t, report.Qualified, "%+v", report)
				require.Len(t, report.Observations, 3)
				baseline := report.Observations[0].State.Audit
				for _, observation := range report.Observations {
					got := observation.State.Audit
					got.TipDigest = baseline.TipDigest
					require.Equal(t, baseline, got, "every audit observation field except the full tip digest must agree")
				}
				publishReports(reports)
			})
			if events == nil {
				return
			}
			require.True(t, sdktest.Find(t, events, "cross-node identity oracle obtains a qualified all-member observation").Condition)
			assertEventCondition(t, events, "account state is identical across nodes at a common applied index", true)
			assertEventCondition(t, events, "audit hash version is identical across nodes at the same sequence", true)
			assertEventCondition(t, events, "complete audit listing and point tip agree at a common applied index", true)
			assertEventCondition(t, events, "audit hash is identical across nodes at the same sequence", matching)
			const success = "cross-node complete account and audit identity verified"
			if matching {
				require.True(t, sdktest.Find(t, events, success).Condition)
			} else {
				sdktest.Absent(t, events, success)
			}
		})
	}
}

func TestIdentitySingleMemberDoesNotClaimCrossNodeCoverage(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	t.Parallel()
	events := sdktest.Capture(t, func() {
		cluster, conns := newIdentityCluster(t, "equal")
		cluster.members = cluster.members[:1]
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		reports := runOracle(ctx, conns, identityConfig())
		require.True(t, reports[len(reports)-1].Qualified, "%+v", reports)
		publishReports(reports)
	})
	if events == nil {
		return
	}
	require.True(t, sdktest.Find(t, events, "cross-node identity oracle obtains a qualified all-member observation").Condition)
	sdktest.Absent(t, events, "account state is identical across nodes at a common applied index")
	sdktest.Absent(t, events, "audit hash is identical across nodes at the same sequence")
	sdktest.Absent(t, events, "cross-node complete account and audit identity verified")
}

// An SDK property may emit once for success and once for failure when three
// replicas are compared. Require the failure if any observation is divergent.
func assertEventCondition(t *testing.T, events []sdktest.Event, property string, expected bool) {
	t.Helper()
	found := false
	for _, event := range events {
		if event.Message != property || !event.Hit {
			continue
		}
		if expected {
			require.True(t, event.Condition, "%s: %+v", property, event)
		} else if !event.Condition {
			found = true
		}
		if expected {
			found = true
		}
	}
	require.True(t, found, "no hit for %q with expected condition %v", property, expected)
}
