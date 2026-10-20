//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func expectProtocolRejection(err error) {
	GinkgoHelper()
	Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
	Expect(status.Convert(err).Message()).To(ContainSubstring("incompatible Ledger gRPC protocol"))
}

var _ = Describe("gRPC protocol version", Ordered, func() {
	var (
		clusterCtx context.Context
		ctx        context.Context
		servers    []*testutil.ServiceWithClient
		leaderID   *uint64
		rawConn    *grpc.ClientConn
		rawClient  servicepb.BucketServiceClient
		leader     *testutil.ServiceWithClient
	)

	BeforeAll(func() {
		clusterCtx, servers, _, leaderID = testutil.SetupMultiNodeCluster(3)
		leader = servers[*leaderID-1]
		// Deliberately omit grpcprotocol.ClientOption to represent an old client.
		var err error
		rawConn, err = grpc.NewClient(fmt.Sprintf("localhost:%d", leader.GRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).To(Succeed())
		DeferCleanup(func() { Expect(rawConn.Close()).To(Succeed()) })
		rawClient = servicepb.NewBucketServiceClient(rawConn)
	})

	AfterAll(func() {
		// Stop nodes before the helper's deferred directory cleanup runs.
		testutil.StopServers(context.Background(), servers)
	})

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(clusterCtx, 10*time.Second)
		DeferCleanup(cancel)
	})

	DescribeTable("rejects incompatible writes before creating a ledger", func(name string, versions []string) {
		requestCtx := ctx
		if versions != nil {
			requestCtx = metadata.NewOutgoingContext(ctx, metadata.MD{grpcprotocol.MetadataKey: versions})
		}
		ledgerName := "protocol-" + name
		req := servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil))
		_, err := rawClient.Apply(requestCtx, req)
		expectProtocolRejection(err)

		// A real, valid write must leave no business effect when refused.
		_, err = leader.Client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
		Expect(status.Code(err)).To(Equal(codes.NotFound))

		// Submit the identical payload with the supported protocol and observe it.
		resp, err := leader.Client.Apply(ctx, req)
		Expect(err).To(Succeed())
		Expect(resp.Logs).To(HaveLen(1))
		ledgerInfo, err := leader.Client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
		Expect(err).To(Succeed())
		Expect(ledgerInfo.Name).To(Equal(ledgerName))
	},
		Entry("without protocol metadata", "missing", []string(nil)),
		Entry("with a different protocol", "different", []string{"0"}),
		Entry("with an invalid protocol", "invalid", []string{"dev"}),
		Entry("with duplicate protocol metadata", "duplicate", []string{grpcprotocol.Version, grpcprotocol.Version}),
	)

	It("checks each stream and Cluster RPC while allowing diagnostic RPCs", func() {
		info, err := rawClient.Discovery(ctx, &servicepb.DiscoveryRequest{})
		Expect(err).To(Succeed())
		Expect(info.GetServerInfo().GetProtocolVersion()).To(Equal(grpcprotocol.Version))
		// Health can report NOT_SERVING while projections warm up. The contract
		// here is that the diagnostic RPC remains callable without a revision.
		_, err = grpc_health_v1.NewHealthClient(rawConn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		Expect(err).To(Succeed())

		// Successful discovery on this connection does not authorize later RPCs.
		_, err = clusterpb.NewClusterServiceClient(rawConn).GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		expectProtocolRejection(err)
		stream, err := rawClient.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
		if err == nil {
			_, err = stream.Recv()
		}
		expectProtocolRejection(err)

		_, err = leader.Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("protocol-stream", nil)))
		Expect(err).To(Succeed())
		ledgers, err := actions.ListLedgers(ctx, leader.Client)
		Expect(err).To(Succeed())
		Expect(ledgers).To(HaveKey("protocol-stream"))
		state, err := leader.ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		Expect(err).To(Succeed())
		Expect(state.Nodes).To(HaveLen(3))
	})

	It("forwards an HTTP write from a follower with the server's protocol version", func() {
		currentLeaderID := *leaderID
		follower := servers[currentLeaderID%uint64(len(servers))]
		Expect(uint64(follower.NodeID)).NotTo(Equal(currentLeaderID))
		state, err := follower.ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		Expect(err).To(Succeed())
		Expect(uint64(state.Leader)).To(Equal(currentLeaderID))

		const ledgerName = "protocol-http-forwarding"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			fmt.Sprintf("http://localhost:%d/v3/%s", follower.HTTPPort, ledgerName), nil)
		Expect(err).To(Succeed())
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer func() { Expect(resp.Body.Close()).To(Succeed()) }()
		body, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusCreated), string(body))

		ledgerInfo, err := servers[currentLeaderID-1].Client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
		Expect(err).To(Succeed())
		Expect(ledgerInfo.Name).To(Equal(ledgerName))
		state, err = follower.ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		Expect(err).To(Succeed())
		Expect(uint64(state.Leader)).To(Equal(currentLeaderID), "the request must have used the follower-to-leader path")
	})
})
