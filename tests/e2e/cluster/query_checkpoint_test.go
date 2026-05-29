//go:build e2e

package cluster

import (
	"context"
	"io"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = Describe("Query Checkpoints", func() {

	Context("Create, query, and delete checkpoints", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9220
			grpcPort   = 8220
			ledgerName = "qcp-test"
		)

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			// Create a ledger.
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("should list no checkpoints initially", func() {
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCheckpoints()).To(BeEmpty())
		})

		var checkpointID uint64

		It("should create a transaction before the checkpoint", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(1000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("should create a query checkpoint with sequential ID", func() {
			resp, err := clusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCheckpointId()).To(Equal(uint64(1))) // First checkpoint gets ID 1
			Expect(resp.GetMaxSequence()).NotTo(BeZero())

			checkpointID = resp.GetCheckpointId()
		})

		It("should list the checkpoint", func() {
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCheckpoints()).To(HaveLen(1))
			Expect(resp.GetCheckpoints()[0].GetCheckpointId()).To(Equal(checkpointID))
		})

		It("should get checkpoint info", func() {
			info, err := clusterClient.GetQueryCheckpointInfo(ctx, &clusterpb.GetQueryCheckpointInfoRequest{
				CheckpointId: checkpointID,
			})
			Expect(err).To(Succeed())
			Expect(info.GetCheckpointId()).To(Equal(checkpointID))
			Expect(info.GetMaxSequence()).NotTo(BeZero())
			Expect(info.GetCreatedAt()).NotTo(BeNil())
		})

		It("should create a post-checkpoint transaction", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(500), "EUR"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("should see both transactions in the live store", func() {
			txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
		})

		It("should see only the pre-checkpoint transaction when querying the checkpoint", func() {
			txs, err := listAllTransactionsFromCheckpoint(ctx, client, ledgerName, 100, 0, checkpointID)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(1))

			// The only transaction should be the alice/USD one.
			Expect(txs[0].GetPostings()).To(HaveLen(1))
			Expect(txs[0].GetPostings()[0].GetDestination()).To(Equal("alice"))
		})

		It("should read a single transaction from the checkpoint", func() {
			// First get the transaction list from the checkpoint to know the actual ID.
			txs, err := listAllTransactionsFromCheckpoint(ctx, client, ledgerName, 100, 0, checkpointID)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(1))

			txID := txs[0].GetId()

			resp, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: txID,
				CheckpointId:  checkpointID,
			})
			Expect(err).To(Succeed())
			Expect(resp.GetTransaction().GetPostings()[0].GetDestination()).To(Equal("alice"))
		})

		It("should delete the checkpoint", func() {
			_, err := clusterClient.DeleteQueryCheckpoint(ctx, &clusterpb.DeleteQueryCheckpointRequest{
				CheckpointId: checkpointID,
			})
			Expect(err).To(Succeed())
		})

		It("should list no checkpoints after deletion", func() {
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCheckpoints()).To(BeEmpty())
		})
	})

	Context("Multiple checkpoints capture progressive state", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9221
			grpcPort   = 8221
			ledgerName = "qcp-progressive"
		)

		var checkpoint1ID, checkpoint2ID uint64

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("should create tx1, checkpoint1, tx2, checkpoint2, tx3", func() {
			// tx0: world -> alice 100
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Checkpoint 1
			resp, err := clusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
			Expect(err).To(Succeed())
			checkpoint1ID = resp.GetCheckpointId()

			// tx1: world -> bob 200
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Checkpoint 2
			resp, err = clusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
			Expect(err).To(Succeed())
			checkpoint2ID = resp.GetCheckpointId()

			// tx2: world -> charlie 300
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "charlie", big.NewInt(300), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("should list two checkpoints with sequential IDs", func() {
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCheckpoints()).To(HaveLen(2))
			Expect(checkpoint1ID).To(Equal(uint64(1)))
			Expect(checkpoint2ID).To(Equal(uint64(2)))
		})

		It("checkpoint1 should see 1 transaction", func() {
			txs, err := listAllTransactionsFromCheckpoint(ctx, client, ledgerName, 100, 0, checkpoint1ID)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(1))
			Expect(txs[0].GetPostings()[0].GetDestination()).To(Equal("alice"))
		})

		It("checkpoint2 should see 2 transactions", func() {
			txs, err := listAllTransactionsFromCheckpoint(ctx, client, ledgerName, 100, 0, checkpoint2ID)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
		})

		It("live store should see 3 transactions", func() {
			txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(3))
		})

		It("deleting checkpoint1 should not affect checkpoint2", func() {
			_, err := clusterClient.DeleteQueryCheckpoint(ctx, &clusterpb.DeleteQueryCheckpointRequest{
				CheckpointId: checkpoint1ID,
			})
			Expect(err).To(Succeed())

			// Checkpoint2 should still work.
			txs, err := listAllTransactionsFromCheckpoint(ctx, client, ledgerName, 100, 0, checkpoint2ID)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
		})
	})
})

// listAllTransactionsFromCheckpoint collects all transactions from a checkpoint via the streaming RPC.
func listAllTransactionsFromCheckpoint(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, afterTxID uint64, checkpointID uint64) ([]*commonpb.Transaction, error) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:       ledgerName,
		PageSize:     pageSize,
		AfterTxId:    afterTxID,
		CheckpointId: checkpointID,
	})
	if err != nil {
		return nil, err
	}

	var transactions []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}
