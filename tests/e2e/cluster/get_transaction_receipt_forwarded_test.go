//go:build e2e

package cluster

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/metadata"
)

// A GetTransaction receipt is signed by whichever node actually reads the
// transaction. When the read is routed to the leader (Consistency: leader), the
// contacted node must relay the leader-signed receipt verbatim — even when that
// contacted node has no receipt signer of its own.
//
// This spec configures only the leader (the bootstrap node) with a
// receipt-signing key and leaves the followers without one. A default read
// served locally by a no-signer follower therefore returns an empty receipt;
// the same read with Consistency: leader forwards to the leader, which signs,
// and the follower must relay it. The two together confirm the receipt comes
// from the forwarded leader, and that the no-signer follower does not drop it.
var _ = Describe("GetTransaction forwarded receipt (heterogeneous signer)", Ordered, func() {
	const (
		countInstances = 3
		ledgerName     = "forwarded-receipt-hetero"
		receiptKey     = "test-receipt-signing-key"
	)

	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		leaderID *uint64
	)

	BeforeAll(func() {
		// Only node 0 — the bootstrap node, which becomes leader — gets a
		// receipt-signing key; the followers get none.
		ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(
			countInstances, testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
			testutil.WithNodeInstruments(0, testserver.WithReceiptSigningKey(receiptKey)),
		)

		_, err := servers[*leaderID-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("relays the leader's receipt on a Consistency: leader read from a follower with no signer", func() {
		lid := *leaderID
		followerID := ((lid + 1) % countInstances) + 1
		Expect(followerID).NotTo(Equal(lid))
		leaderClient := servers[lid-1].Client
		followerClient := servers[followerID-1].Client

		applyResp, err := leaderClient.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil, nil)))
		Expect(err).To(Succeed())
		Expect(applyResp.Logs).To(HaveLen(1))
		txID := applyResp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()

		// A read served locally by the no-signer follower (default consistency, so
		// it serves after its own read barrier) finds the transaction but cannot
		// sign a receipt — establishing that a non-empty receipt below can only
		// have come from the forwarded leader.
		localResp, err := followerClient.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: ledgerName, TransactionId: txID})
		Expect(err).To(Succeed())
		Expect(localResp.GetTransaction().GetId()).To(Equal(txID))
		Expect(localResp.GetReceipt()).To(BeEmpty(), "a no-signer follower cannot sign a receipt for a locally-served read")

		// The behavior under test: Consistency: leader forwards to the leader,
		// which signs; the no-signer follower must relay that receipt rather than
		// drop it because it lacks a signer of its own.
		leaderCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "leader")
		fwdResp, err := followerClient.GetTransaction(leaderCtx, &servicepb.GetTransactionRequest{Ledger: ledgerName, TransactionId: txID})
		Expect(err).To(Succeed())
		Expect(fwdResp.GetTransaction().GetId()).To(Equal(txID))
		Expect(fwdResp.GetReceipt()).NotTo(BeEmpty(),
			"a no-signer follower must relay the leader-signed receipt on a forwarded read")
	})
})
