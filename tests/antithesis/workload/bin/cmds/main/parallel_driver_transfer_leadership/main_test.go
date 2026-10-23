package main

import (
	"context"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

// The fixture changes acknowledged business data or RPC outcomes. It does not
// replace the driver's assertions or its sentinel helper.
type transferFixture struct {
	servicepb.UnimplementedBucketServiceServer
	clusterpb.UnimplementedClusterServiceServer

	mu        sync.Mutex
	scenario  string
	leader    uint64
	writes    int
	transfers int
	reads     []uint64
	data      map[uint64]*commonpb.Transaction
}

func (f *transferFixture) GetClusterState(context.Context, *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scenario == "new election after writes" && f.writes == 2 {
		return &clusterpb.ClusterState{}, nil
	}

	return &clusterpb.ClusterState{Leader: f.leader, Nodes: []*clusterpb.NodeInfo{
		{Id: 1, Suffrage: "Voter"}, {Id: 2, Suffrage: "Voter"}, {Id: 3, Suffrage: "Learner"},
	}}, nil
}

func (f *transferFixture) TransferLeadership(_ context.Context, request *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.GetTransferee() != 2 || len(f.data) != 1 {
		return nil, status.Error(codes.InvalidArgument, "transfer must follow the acknowledged witness and target the other voter")
	}
	f.transfers++
	if f.scenario == "transfer unavailable" {
		return nil, status.Error(codes.Unavailable, "transfer outcome unknown")
	}
	f.leader = 2
	if f.scenario == "wrong target" {
		return &clusterpb.TransferLeadershipResponse{NewLeader: 3}, nil
	}

	return &clusterpb.TransferLeadershipResponse{NewLeader: 2}, nil
}

func (f *transferFixture) Apply(_ context.Context, request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	requests := request.GetUnsigned().GetRequests()
	if len(requests) != 1 {
		return nil, status.Error(codes.InvalidArgument, "one witness operation expected")
	}
	if requests[0].GetCreateLedger() != nil {
		return &servicepb.ApplyResponse{}, nil
	}
	apply := requests[0].GetApply()
	payload := apply.GetAction().GetCreateTransaction()
	if apply.GetLedger() != "sentinel-leadership-transfer" || payload == nil {
		return nil, status.Error(codes.InvalidArgument, "wrong witness operation")
	}
	f.writes++
	if f.scenario == "prewrite unavailable" && f.writes == 1 || f.scenario == "postwrite unavailable" && f.writes == 2 {
		return nil, status.Error(codes.Unavailable, "witness outcome unknown")
	}
	id := uint64(f.writes)
	tx := &commonpb.Transaction{Id: id, Reference: payload.GetReference(), Postings: payload.GetPostings(), Metadata: payload.GetMetadata()}
	f.data[id] = proto.Clone(tx).(*commonpb.Transaction)

	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: apply.GetLedger(),
			Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: tx},
			}}},
		}}},
	}}}, nil
}

func (f *transferFixture) GetLedger(context.Context, *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	return &commonpb.LedgerInfo{Name: "sentinel-leadership-transfer"}, nil
}

func (f *transferFixture) GetTransaction(_ context.Context, request *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := request.GetTransactionId()
	f.reads = append(f.reads, id)
	if f.scenario == "lost prewrite" && id == 1 {
		return nil, status.Error(codes.NotFound, "acknowledged witness disappeared")
	}
	tx := proto.Clone(f.data[id]).(*commonpb.Transaction)
	if f.scenario == "corrupt prewrite" && id == 1 || f.scenario == "corrupt postwrite" && id == 2 {
		tx.Postings[0] = commonpb.NewPosting("world", "wrong-destination", "COIN", big.NewInt(0))
	}

	return &servicepb.GetTransactionResponse{Transaction: tx}, nil
}

func (f *transferFixture) clients(t *testing.T) (clusterpb.ClusterServiceClient, servicepb.BucketServiceClient) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, f)
	clusterpb.RegisterClusterServiceServer(server, f)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-done) })
	conn, err := grpc.NewClient("passthrough:///transfer-fixture", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return clusterpb.NewClusterServiceClient(conn), servicepb.NewBucketServiceClient(conn)
}

func TestTransferSentinelSDK(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	t.Parallel()
	const content = "committed sentinel transaction content survives operational events"
	const progress = "business write commits after leadership transfer"
	for _, scenario := range []string{"healthy", "corrupt prewrite", "corrupt postwrite", "lost prewrite", "prewrite unavailable", "postwrite unavailable", "transfer unavailable", "wrong target", "new election after writes"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			events := sdktest.Capture(t, func() {
				fixture := &transferFixture{scenario: scenario, leader: 1, data: make(map[uint64]*commonpb.Transaction)}
				cluster, bucket := fixture.clients(t)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				runTransfer(ctx, cluster, bucket)
				fixture.mu.Lock()
				defer fixture.mu.Unlock()
				switch scenario {
				case "prewrite unavailable":
					require.Zero(t, fixture.transfers)
					require.Empty(t, fixture.reads)
				case "transfer unavailable":
					require.Equal(t, 1, fixture.transfers)
					require.Equal(t, 1, fixture.writes)
				case "postwrite unavailable":
					require.Equal(t, []uint64{1}, fixture.reads)
				default:
					require.Equal(t, 1, fixture.transfers)
					require.Equal(t, 2, fixture.writes)
					require.Equal(t, []uint64{1, 2}, fixture.reads)
				}
			})
			if events == nil {
				return
			}
			switch scenario {
			case "prewrite unavailable":
				sdktest.Absent(t, events, "leadership transfer completed")
				sdktest.Absent(t, events, progress)
			case "transfer unavailable":
				require.False(t, sdktest.Find(t, events, "should be able to transfer leadership").Condition)
				sdktest.Absent(t, events, progress)
			case "postwrite unavailable":
				require.False(t, sdktest.Find(t, events, progress).Condition)
			case "lost prewrite":
				require.False(t, sdktest.Find(t, events, "committed sentinel transaction must survive operational events").Condition)
			case "wrong target":
				require.False(t, sdktest.Find(t, events, "new leader should be the requested target").Condition)
			case "new election after writes":
				event := sdktest.Find(t, events, "cluster should have a leader after transfer")
				require.Equal(t, "Sometimes", event.DisplayType)
				require.False(t, event.Condition)
				require.True(t, sdktest.Find(t, events, progress).Condition)
				for _, event := range events {
					if event.DisplayType == "Always" || event.DisplayType == "AlwaysOrUnreachable" {
						require.True(t, event.Condition, "a new election is permitted while faults remain active")
					}
				}
			case "corrupt prewrite", "corrupt postwrite":
				found := false
				for _, event := range events {
					if event.Message == content && event.Hit && !event.Condition {
						found = true
					}
				}
				require.True(t, found, "content corruption must remain visible even if another witness passes")
			default:
				require.True(t, sdktest.Find(t, events, content).Condition)
				require.True(t, sdktest.Find(t, events, progress).Condition)
			}
		})
	}
}
