//go:build e2e

package e2e

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CreateCheckpoint", Ordered, func() {
	var (
		ctx           context.Context
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, clusterClient = setupSingleNode(httpPort, grpcPort)

		// Create a ledger with some data so the checkpoint is non-trivial
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{createLedgerAction("checkpoint-test", nil)},
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction("checkpoint-test", []*commonpb.Posting{
					newPosting("world", "bank", big.NewInt(10000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
	})

	It("should create a checkpoint and return a valid checkpoint ID", func() {
		resp, err := clusterClient.CreateCheckpoint(ctx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())
		Expect(resp.CheckpointId).To(BeNumerically(">", 0))
	})

	It("should create monotonically increasing checkpoint IDs", func() {
		resp1, err := clusterClient.CreateCheckpoint(ctx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())

		resp2, err := clusterClient.CreateCheckpoint(ctx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())

		Expect(resp2.CheckpointId).To(BeNumerically(">", resp1.CheckpointId))
	})

	It("should not interfere with normal cluster operations", func() {
		_, err := clusterClient.CreateCheckpoint(ctx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())

		// Verify the cluster is still healthy after checkpoint
		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		Expect(err).To(Succeed())
		Expect(state.Leader).NotTo(BeZero())

		// Verify we can still create transactions after checkpoint
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction("checkpoint-test", []*commonpb.Posting{
					newPosting("world", "user", big.NewInt(500), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
	})
})
