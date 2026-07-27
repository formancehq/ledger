//go:build e2e

package business

import (
	"context"
	"math/big"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// Once a transaction's chapter is archived, its creation log is purged from hot
// storage. GetTransaction rebuilds the transaction from that log (the
// attribute-zone state carries no reference / post-commit volumes), so without a
// cold-storage fallback it returns NotFound for an archived transaction — even
// though the log is safe in cold storage and GetLog can still read it. This
// pins that GetTransaction (and its receipt) fall back to cold storage, so an
// archived transaction stays fully readable.
var _ = Describe("GetTransaction reads archived transactions from cold storage", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = 15702
		grpcPort = 15802
		ledger   = "get-tx-cold"
		txRef    = "cold-tx-ref"
		txID     = 1 // first transaction in a fresh ledger
	)

	BeforeAll(func() {
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort,
			testserver.WithColdStorageDriver("filesystem"),
			testserver.WithReceiptSigningKey("get-tx-cold-receipt-key"),
		)
	})

	It("returns the full transaction and its receipt after its chapter is archived", func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		// A referenced transaction: reference lives only in the creation log, so
		// it is the field a state-only rebuild would lose.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.WithReference(
				actions.CreateTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
				}, nil, nil),
				txRef,
			),
		))
		Expect(err).To(Succeed())

		// Baseline read while the chapter is still hot.
		hot, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: txID})
		Expect(err).To(Succeed())
		Expect(hot.GetTransaction().GetReference()).To(Equal(txRef))
		Expect(hot.GetTransaction().GetPostCommitVolumes()).ToNot(BeNil(), "post-commit volumes present while hot")
		Expect(hot.GetReceipt()).ToNot(BeEmpty(), "receipt present while hot")

		// Close + seal + archive + confirm the chapter holding the transaction,
		// purging its creation log from hot storage into cold.
		archiveChapterFull(ctx, client)

		// The read that regresses without the cold fallback: NotFound before the
		// fix. With it, the transaction comes back whole from cold storage.
		got, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: txID})
		Expect(err).To(Succeed(), "archived transaction must still be readable from cold storage")

		tx := got.GetTransaction()
		Expect(tx.GetId()).To(Equal(uint64(txID)))
		Expect(tx.GetReference()).To(Equal(txRef), "reference must survive archival (log-only field)")
		Expect(tx.GetPostings()).To(HaveLen(1))
		Expect(tx.GetPostings()[0].GetSource()).To(Equal("world"))
		Expect(tx.GetPostings()[0].GetDestination()).To(Equal("users:alice"))
		Expect(tx.GetPostCommitVolumes()).ToNot(BeNil(), "post-commit volumes must survive archival (log-only field)")

		// The receipt lookup shares the cold fallback, so an archived transaction
		// keeps its receipt (the chapter id it signs over is immutable).
		Expect(got.GetReceipt()).ToNot(BeEmpty(), "receipt must survive archival via the cold fallback")
	})
})
