//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CreateCheckpoint", Ordered, func() {

	BeforeAll(func() {

		// Create a ledger with some data so the checkpoint is non-trivial
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction("checkpoint-test", nil)),
		})
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction("checkpoint-test", []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(10000), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())
	})

	It("should create a checkpoint and return a valid checkpoint ID", func() {
		resp, err := sharedClusterClient.CreateCheckpoint(sharedCtx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())
		Expect(resp.CheckpointId).To(BeNumerically(">", 0))
	})

	It("should create monotonically increasing checkpoint IDs", func() {
		resp1, err := sharedClusterClient.CreateCheckpoint(sharedCtx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())

		resp2, err := sharedClusterClient.CreateCheckpoint(sharedCtx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())

		Expect(resp2.CheckpointId).To(BeNumerically(">", resp1.CheckpointId))
	})

	It("should not interfere with normal cluster operations", func() {
		_, err := sharedClusterClient.CreateCheckpoint(sharedCtx, &clusterpb.CreateCheckpointRequest{})
		Expect(err).To(Succeed())

		// Verify the cluster is still healthy after checkpoint
		state, err := sharedClusterClient.GetClusterState(sharedCtx, &clusterpb.GetClusterStateRequest{})
		Expect(err).To(Succeed())
		Expect(state.Leader).NotTo(BeZero())

		// Verify we can still create transactions after checkpoint
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction("checkpoint-test", []*commonpb.Posting{
					actions.NewPosting("world", "user", big.NewInt(500), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())
	})
})
