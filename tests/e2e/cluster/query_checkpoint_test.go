//go:build e2e

package cluster

import (
	"context"
	"io"
	"math/big"
	"strconv"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("should list no checkpoints initially", func() {
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCheckpoints()).To(BeEmpty())
		})

		var checkpointID uint64

		It("should create a transaction before the checkpoint", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(1000), "USD"),
			}, nil)))
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
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bob", big.NewInt(500), "EUR"),
			}, nil)))
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

		It("GetAccount reads checkpoint state, not live", func() {
			// bob was funded (500 EUR) AFTER the checkpoint.
			liveBob, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "bob"})
			Expect(err).To(Succeed())
			Expect(liveBob.FindVolume("EUR", "")).NotTo(BeNil(), "live store has the post-checkpoint balance")

			cpBob, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:       ledgerName,
				Address:      "bob",
				CheckpointId: checkpointID,
			})
			Expect(err).To(Succeed())
			Expect(cpBob.FindVolume("EUR", "")).To(BeNil(),
				"checkpoint predates bob; reading it must not return live data")
		})

		It("GetLedgerStats reads checkpoint state, not live", func() {
			liveStats, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(liveStats.GetTransactionCount()).To(Equal(uint64(2)), "live store has both transactions")

			cpStats, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{
				Ledger:       ledgerName,
				CheckpointId: checkpointID,
			})
			Expect(err).To(Succeed())
			Expect(cpStats.GetTransactionCount()).To(Equal(uint64(1)),
				"checkpoint stats must reflect only the pre-checkpoint transaction")
		})

		It("AggregateVolumes reads checkpoint state, not live", func() {
			liveAgg, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(aggregateAssets(liveAgg)).To(ContainElements("USD", "EUR"), "live store has both assets")

			cpAgg, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
				Ledger:       ledgerName,
				CheckpointId: checkpointID,
			})
			Expect(err).To(Succeed())
			Expect(aggregateAssets(cpAgg)).To(ConsistOf("USD"),
				"checkpoint must omit the post-checkpoint EUR volume")
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

			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("should create tx1, checkpoint1, tx2, checkpoint2, tx3", func() {
			// tx0: world -> alice 100
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil)))
			Expect(err).To(Succeed())

			// Checkpoint 1
			resp, err := clusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
			Expect(err).To(Succeed())
			checkpoint1ID = resp.GetCheckpointId()

			// tx1: world -> bob 200
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
			}, nil)))
			Expect(err).To(Succeed())

			// Checkpoint 2
			resp, err = clusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
			Expect(err).To(Succeed())
			checkpoint2ID = resp.GetCheckpointId()

			// tx2: world -> charlie 300
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "charlie", big.NewInt(300), "USD"),
			}, nil)))
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

	// Regression coverage for #279: GetAccount(checkpoint_id=N) was returning
	// live volumes regardless of the checkpoint. The fix landed in #246 along
	// with a single-shot regression test ("GetAccount reads checkpoint state,
	// not live", earlier in this file), but that test covers only the case
	// of an account that does not yet exist at the checkpoint — a leaked
	// live read still looks "wrong but not catastrophic" there because the
	// missing-volume signal is binary. The issue's actual reproduction was
	// the opposite: an account that already exists at the checkpoint with a
	// partial balance, then receives further credits. This context credits
	// the SAME account across three checkpoints and asserts each checkpoint
	// returns its own frozen balance (100, 350, 400). A future regression
	// in the scan layer (e.g. accumulating live entries on top of checkpoint
	// entries) would surface here even if the simpler test still passed.
	Context("GetAccount(checkpoint_id) returns frozen balance for the same account across checkpoints", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9222
			grpcPort   = 8222
			ledgerName = "qcp-progressive-balance"
			acc        = "users:test"
			asset      = "USD"
		)

		var cpA, cpB, cpC uint64

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("builds three balance steps with a checkpoint after each credit", func() {
			credit := func(amount int64) {
				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", acc, big.NewInt(amount), asset),
				}, nil)))
				Expect(err).To(Succeed())
			}

			snapshot := func() uint64 {
				resp, err := clusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
				Expect(err).To(Succeed())

				return resp.GetCheckpointId()
			}

			credit(100)
			cpA = snapshot() // expected balance @ cpA = 100

			credit(250)
			cpB = snapshot() // expected balance @ cpB = 350

			credit(50)
			cpC = snapshot() // expected balance @ cpC = 400
		})

		assertBalance := func(cp uint64, expected string) {
			GinkgoHelper()

			resp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:       ledgerName,
				Address:      acc,
				CheckpointId: cp,
			})
			Expect(err).To(Succeed())

			vols := resp.FindVolume(asset, "")
			Expect(vols).NotTo(BeNil(), "expected %s volumes at checkpoint %d", asset, cp)
			Expect(vols.GetBalance()).To(Equal(expected),
				"balance at checkpoint %d should be frozen at %s", cp, expected)
		}

		It("checkpoint A returns the balance after the first credit only (100)", func() {
			assertBalance(cpA, "100")
		})

		It("checkpoint B returns the balance after the first two credits (350)", func() {
			assertBalance(cpB, "350")
		})

		It("checkpoint C returns the balance after all three credits (400)", func() {
			assertBalance(cpC, "400")
		})

		It("live store returns the current balance (400)", func() {
			resp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: acc,
			})
			Expect(err).To(Succeed())

			Expect(resp.FindVolume(asset, "").GetBalance()).To(Equal("400"))
		})
	})

	// EN-1460: an immediate read at a freshly-created checkpoint used to race the
	// asynchronous read-index materialization and return an opaque, non-retryable
	// code=Unknown. CreateQueryCheckpoint now blocks on the CREATOR node's local
	// .ready marker, so a read routed back to that node succeeds immediately.
	// This single-node suite always hits the creator, so the read must succeed
	// with zero delay — and any error must be a typed retryable Unavailable,
	// never an opaque Unknown.
	Context("immediate read after checkpoint creation is race-free", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9224
			grpcPort   = 8224
			ledgerName = "qcp-race"
		)

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("reads the checkpoint immediately on the creator node, never Unknown", func() {
			// Repeat create-then-read several times to shrink the odds the former
			// race window is simply missed by chance.
			for i := 0; i < 10; i++ {
				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
				}, nil)))
				Expect(err).To(Succeed())

				// CreateQueryCheckpoint blocks until the read index is
				// materialized on this (creator) node.
				resp, err := clusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
				Expect(err).To(Succeed())
				cpID := resp.GetCheckpointId()

				// Read at the checkpoint with zero delay on the creator node.
				// Before the fix this intermittently returned code=Unknown; now
				// it must succeed because Create waited on the marker.
				agg, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
					Ledger:       ledgerName,
					CheckpointId: cpID,
				})
				Expect(err).To(Succeed(),
					"read on the creator node must succeed immediately after Create returns (EN-1460); got %v", err)
				Expect(aggregateAssets(agg)).To(ContainElement("USD"))
			}
		})
	})
})

// EN-1460 (multi-node): the read-index checkpoint is materialized per-replica by
// each node's index builder. A checkpoint read routed to a node whose builder
// has not yet crossed the CreatedQueryCheckpoint log returns a typed retryable
// Unavailable (CHECKPOINT_NOT_READY), never an opaque Unknown, and every node
// eventually materializes the checkpoint and serves the read.
var _ = Describe("Query Checkpoints (multi-node readiness)", Ordered, func() {
	const (
		countInstances = 3
		ledgerName     = "qcp-multinode"
	)

	var (
		ctx     context.Context
		servers []*testutil.ServiceWithClient
	)

	BeforeAll(func() {
		ctx, servers, _, _ = testutil.SetupMultiNodeCluster(
			countInstances,
			testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
		)

		_, err := servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		_, err = servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
		}, nil)))
		Expect(err).To(Succeed())
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("serves the checkpoint on every node — retryable, never Unknown, eventually consistent", func() {
		resp, err := servers[0].ClusterClient.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
		Expect(err).To(Succeed())
		cpID := resp.GetCheckpointId()

		for i := range servers {
			node := servers[i]

			// Any read may transiently fail while this node's builder catches
			// up, but only with the typed retryable Unavailable — never Unknown.
			// It must eventually succeed on every node (per-replica
			// materialization).
			Eventually(func(g Gomega) {
				agg, aggErr := node.Client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
					Ledger:       ledgerName,
					CheckpointId: cpID,
				})
				if aggErr != nil {
					g.Expect(status.Code(aggErr)).To(Equal(codes.Unavailable),
						"pre-ready checkpoint read must be retryable Unavailable, never Unknown; got %v", aggErr)
					g.Expect(aggErr).To(HaveOccurred()) // force retry
					return
				}

				g.Expect(aggregateAssets(agg)).To(ContainElement("USD"))
			}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
		}
	})

	It("returns NotFound (not Unavailable) for a checkpoint id that was never created", func() {
		_, err := servers[0].Client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
			Ledger:       ledgerName,
			CheckpointId: 999999,
		})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.NotFound),
			"a non-existent checkpoint id must be NotFound (permanent), not Unavailable; got %v", err)
	})
})

// listAllTransactionsFromCheckpoint collects all transactions from a checkpoint via the streaming RPC.
func listAllTransactionsFromCheckpoint(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, afterTxID uint64, checkpointID uint64) ([]*commonpb.Transaction, error) {
	var cursor string
	if afterTxID > 0 {
		cursor = strconv.FormatUint(afterTxID, 10)
	}
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger: ledgerName,
		Options: &commonpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursor,
			Read:     &commonpb.ReadOptions{CheckpointId: checkpointID},
		},
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

// aggregateAssets returns the asset codes present in an AggregateVolumes result.
func aggregateAssets(result *commonpb.AggregateResult) []string {
	assets := make([]string, 0, len(result.GetVolumes()))
	for _, v := range result.GetVolumes() {
		assets = append(assets, v.GetAsset())
	}

	return assets
}
