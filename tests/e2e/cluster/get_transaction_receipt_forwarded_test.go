//go:build e2e

package cluster

import (
	"context"
	"math/big"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// A GetTransaction receipt is signed by whichever node actually reads the
// transaction. Default reads normally execute locally after ReadIndex, but a
// follower whose ReadIndex becomes invalid during a leadership change falls
// back to the new leader. The contacted no-signer follower must relay that
// leader-signed receipt verbatim.
var _ = Describe("GetTransaction fallback receipt (heterogeneous signer)", Ordered, func() {
	const (
		countInstances  = 3
		ledgerName      = "forwarded-receipt-hetero"
		receiptKey      = "test-receipt-signing-key"
		noSignerNodeID  = uint64(2)
		readIndexWindow = 10 * time.Second
	)

	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		gateway  *testserver.Gateway
		leaderID *uint64
	)

	BeforeAll(func() {
		// Nodes 1 and 3 can sign; node 2 is deliberately unable to. Keeping two
		// signers lets the test transfer leadership while node 2 remains the
		// contacted follower.
		ctx, servers, gateway, leaderID = testutil.SetupMultiNodeCluster(
			countInstances,
			testutil.WithGateway(),
			testutil.WithNodeInstruments(0, testserver.WithReceiptSigningKey(receiptKey)),
			testutil.WithNodeInstruments(2, testserver.WithReceiptSigningKey(receiptKey)),
		)

		if *leaderID == noSignerNodeID {
			resp, err := servers[*leaderID-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
				Transferee: 1,
			})
			Expect(err).To(Succeed())
			*leaderID = uint64(resp.GetNewLeader())
		}

		_, err := servers[*leaderID-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("relays a leader receipt after the follower's ReadIndex is invalidated", func() {
		lid := *leaderID
		Expect(lid).NotTo(Equal(noSignerNodeID))
		targetID := uint64(1)
		if lid == targetID {
			targetID = 3
		}

		leaderClient := servers[lid-1].Client
		followerClient := servers[noSignerNodeID-1].Client

		applyResp, err := leaderClient.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil, nil)))
		Expect(err).To(Succeed())
		Expect(applyResp.Logs).To(HaveLen(1))
		txID := applyResp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()

		// First prove the no-signer follower can answer locally and cannot create
		// a receipt itself. Any non-empty receipt from the second request must
		// therefore have crossed the fallback hop.
		localResp, err := followerClient.GetTransaction(ctx, &servicepb.GetTransactionRequest{
			Ledger: ledgerName, TransactionId: txID,
		})
		Expect(err).To(Succeed())
		Expect(localResp.GetTransaction().GetId()).To(Equal(txID))
		Expect(localResp.GetReceipt()).To(BeEmpty())

		readIndexSent := make(chan struct{}, 1)
		electionMessageBlocked := make(chan struct{}, 1)
		gateway.SetInterceptor(testserver.MessageInterceptorFunc(func(msg *raftpb.Message) bool {
			if msg.GetFrom() == noSignerNodeID && msg.GetType() == raftpb.MsgReadIndex {
				select {
				case readIndexSent <- struct{}{}:
				default:
				}

				return false
			}

			// Keep the contacted follower out of the election exchange. It then
			// learns the new leader directly from that leader's heartbeat instead
			// of first entering a leaderless state. This makes the pending request
			// exercise the remote fallback below; a leaderless transition would
			// correctly return ErrNoLeader and let the gRPC client retry instead.
			if msg.GetFrom() == targetID && msg.GetTo() == noSignerNodeID &&
				(msg.GetType() == raftpb.MsgVote || msg.GetType() == raftpb.MsgPreVote) {
				select {
				case electionMessageBlocked <- struct{}{}:
				default:
				}

				return false
			}

			return true
		}))
		DeferCleanup(gateway.RemoveInterceptor)

		type readResult struct {
			response *servicepb.GetTransactionResponse
			err      error
		}
		result := make(chan readResult, 1)
		go func() {
			resp, err := followerClient.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger: ledgerName, TransactionId: txID,
			})
			result <- readResult{response: resp, err: err}
		}()

		Eventually(readIndexSent).Within(readIndexWindow).Should(Receive(),
			"the follower read must have a pending quorum request before leadership changes")

		transfer, err := servers[lid-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: uint32(targetID),
		})
		Expect(err).To(Succeed())
		Expect(transfer.GetNewLeader()).To(Equal(uint32(targetID)))
		Eventually(electionMessageBlocked).Within(readIndexWindow).Should(Receive(),
			"the contacted follower must not observe the election's leaderless intermediate state")
		Eventually(func(g Gomega) uint64 {
			state, stateErr := servers[noSignerNodeID-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
				NodeId: uint32(noSignerNodeID),
			})
			g.Expect(stateErr).To(Succeed())

			return uint64(state.GetLeader())
		}).Within(readIndexWindow).Should(Equal(targetID),
			"the contacted follower must observe the new leader before fallback")

		var forwarded readResult
		Eventually(result).Within(readIndexWindow).Should(Receive(&forwarded),
			"the invalidated ReadIndex must fall back to the new leader")
		Expect(forwarded.err).To(Succeed())
		Expect(forwarded.response.GetTransaction().GetId()).To(Equal(txID))
		Expect(forwarded.response.GetReceipt()).NotTo(BeEmpty(),
			"a no-signer follower must relay the receipt produced by the fallback leader")
	})
})
