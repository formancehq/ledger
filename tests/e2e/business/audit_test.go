//go:build e2e

package business

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"math/big"
	"strconv"

	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// collectAuditEntries collects all audit entries from the streaming RPC.
func collectAuditEntries(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ListAuditEntriesRequest) ([]*auditpb.AuditEntry, error) {
	return actions.ListAuditEntriesWithRequest(ctx, client, req)
}

// decodeOrder unmarshals AuditItem.serialized_order back into a typed Order.
// The audit hash chain hashes the stored bytes directly (cf.
// docs/ops/correctness.md); typed Order access is a display-side concern,
// so any UnmarshalVT failure here is a fatal test invariant violation
// rather than a chain-integrity signal.
func decodeOrder(item *auditpb.AuditItem) *raftcmdpb.Order {
	order := &raftcmdpb.Order{}
	Expect(order.UnmarshalVT(item.GetSerializedOrder())).To(Succeed())

	return order
}

var _ = Describe("Audit Log", Ordered, func() {
	const ledgerName = "audit-test"

	// entriesBeforeTest tracks the number of audit entries before each test
	// so assertions can be relative to the test's own actions.
	var entriesBeforeTest int

	BeforeAll(func() {
		// Create the test ledger (generates 1 audit entry)
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
		})
		Expect(err).To(Succeed())
	})

	// Snapshot the current entry count before each test for relative assertions.
	BeforeEach(func() {
		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		entriesBeforeTest = len(entries)
	})

	It("Should record a success audit entry for a successful transaction", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(entries)).To(Equal(entriesBeforeTest + 1))

		last := entries[len(entries)-1]
		Expect(last.GetSuccess()).NotTo(BeNil(), "expected success outcome")
		Expect(last.GetSuccess().GetMinLogSequence()).NotTo(BeZero())
		Expect(last.Sequence).NotTo(BeZero())
	})

	It("Should record a failure audit entry for insufficient funds", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("empty:account", "bank", big.NewInt(99999), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(HaveOccurred())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(entries)).To(Equal(entriesBeforeTest + 1))

		last := entries[len(entries)-1]
		Expect(last.GetFailure()).NotTo(BeNil(), "expected failure outcome")
		Expect(last.GetFailure().ErrorType).To(Equal(domain.ErrReasonInsufficientFunds))
		Expect(last.GetFailure().Message).NotTo(BeEmpty())
	})

	It("Should filter audit entries by ledger name", func() {
		otherLedger := "audit-other-ledger"

		// Create a second ledger
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(otherLedger, nil)),
		})
		Expect(err).To(Succeed())

		// Create a transaction on the second ledger
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(otherLedger, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		// Filter by the original ledger — should not include the second ledger's entries
		filtered, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			Ledger: ledgerName,
		})
		Expect(err).To(Succeed())

		for _, entry := range filtered {
			Expect(entry.GetLedgers()).NotTo(BeEmpty(), "filtered entry should have ledgers populated")
			found := false
			for _, l := range entry.GetLedgers() {
				if l == ledgerName {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "filtered entry should target ledger %q", ledgerName)
		}

		// Filter by the other ledger — should include at least 2 entries (create + transaction)
		otherFiltered, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			Ledger: otherLedger,
		})
		Expect(err).To(Succeed())
		Expect(len(otherFiltered)).To(BeNumerically(">=", 2))
	})

	It("Should filter audit entries by failures only", func() {
		// Create a successful transaction
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		// Create a failing transaction
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("empty:account", "bank", big.NewInt(99999), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(HaveOccurred())

		// Get failures only — every returned entry must be a failure
		failures, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			FailuresOnly: true,
		})
		Expect(err).To(Succeed())
		Expect(failures).NotTo(BeEmpty())
		for _, entry := range failures {
			Expect(entry.GetFailure()).NotTo(BeNil(), "expected only failure entries")
		}
	})

	It("Should support after_sequence pagination", func() {
		// Create a transaction to ensure we have entries
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		// Get all entries
		allEntries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(allEntries)).To(BeNumerically(">=", 2))

		// Get entries after the first one
		afterSeq := allEntries[0].Sequence
		afterEntries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			Options: &commonpb.ListOptions{
				Cursor: strconv.FormatUint(afterSeq, 10),
			},
		})
		Expect(err).To(Succeed())
		Expect(len(afterEntries)).To(Equal(len(allEntries) - 1))
	})

	It("Should include order details via GetAuditEntry with order_count on list and items on get", func() {
		// Create a ledger — produces a CreateLedger order
		ledgerForOrders := "audit-orders-test"
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerForOrders, nil)),
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.GetOrderCount()).To(Equal(uint32(1)))

		// Get the full entry with items
		full, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(1))
		firstOrder := decodeOrder(full.GetItems()[0])
		Expect(firstOrder.GetCreateLedger()).NotTo(BeNil())
		Expect(firstOrder.GetCreateLedger().GetName()).To(Equal(ledgerForOrders))

		// Create a transaction — produces an Apply/CreateTransaction order
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerForOrders, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(500), "EUR"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		entries, err = collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last = entries[len(entries)-1]
		Expect(last.GetOrderCount()).To(Equal(uint32(1)))

		full, err = sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(1))
		apply := decodeOrder(full.GetItems()[0]).GetApply()
		Expect(apply).NotTo(BeNil())
		Expect(apply.GetLedger()).To(Equal(ledgerForOrders))
		Expect(apply.GetCreateTransaction()).NotTo(BeNil())
	})

	It("Should include signing key ID in items", func() {
		// Create a fresh node for signing tests to avoid interfering with other tests
		sigCtx, sigClient, _ := testutil.SetupSingleNode(9109, 8109)

		// Generate a keypair
		pubKey, privKey, genErr := ed25519.GenerateKey(rand.Reader)
		Expect(genErr).To(Succeed())

		const keyID = "audit-test-key"

		// Register the key (bootstrap: first key can be unsigned)
		_, err := sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.RegisterSigningKeyAction(keyID, pubKey),
			),
		})
		Expect(err).To(Succeed())

		// Create a ledger with a signed request
		signedReq := actions.CreateLedgerAction("signed-ledger", nil)
		signedLedger, err := signing.Sign(signedReq, keyID, privKey)
		Expect(err).To(Succeed())
		_, err = sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
			Envelopes: []*servicepb.Envelope{servicepb.SignedEnvelope(signedLedger)},
		})
		Expect(err).To(Succeed())

		// Verify the audit entry items contain the signing key
		entries, err := collectAuditEntries(sigCtx, sigClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]

		// Get full entry with items
		full, err := sigClient.GetAuditEntry(sigCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(1))
		sig := decodeOrder(full.GetItems()[0]).GetSignature()
		Expect(sig).NotTo(BeNil())
		Expect(sig.GetKeyId()).To(Equal(keyID))

		// Verify unsigned orders have no signature (entries[0] = RegisterSigningKey)
		Expect(len(entries)).To(BeNumerically(">=", 1))
		regFull, err := sigClient.GetAuditEntry(sigCtx, &servicepb.GetAuditEntryRequest{
			Sequence: entries[0].Sequence,
		})
		Expect(err).To(Succeed())
		Expect(regFull.GetItems()).NotTo(BeEmpty())
		Expect(decodeOrder(regFull.GetItems()[0]).GetSignature()).To(BeNil())
	})

	It("Should include multiple items in a batch audit entry", func() {
		// Submit multiple requests in a single Apply (batch)
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "batch:a", big.NewInt(100), "USD"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "batch:b", big.NewInt(200), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.GetOrderCount()).To(Equal(uint32(2)))

		full, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(2))
		Expect(decodeOrder(full.GetItems()[0]).GetApply()).NotTo(BeNil())
		Expect(decodeOrder(full.GetItems()[1]).GetApply()).NotTo(BeNil())
	})

	It("Should get a single entry with items populated", func() {
		// Get all entries first
		allEntries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(allEntries).NotTo(BeEmpty())

		// Get the first entry by sequence
		target := allEntries[0]
		entry, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: target.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(entry.Sequence).To(Equal(target.Sequence))
		Expect(entry.ProposalId).To(Equal(target.ProposalId))
		Expect(len(entry.GetItems())).To(Equal(int(target.GetOrderCount())))
	})

	It("Should have log_sequence=0 for failure items", func() {
		// Create a failing transaction
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("empty:nofunds", "bank", big.NewInt(99999), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(HaveOccurred())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			FailuresOnly: true,
		})
		Expect(err).To(Succeed())
		Expect(entries).NotTo(BeEmpty())

		last := entries[len(entries)-1]
		Expect(last.GetFailure()).NotTo(BeNil())

		full, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).NotTo(BeEmpty())
		for _, item := range full.GetItems() {
			Expect(item.GetLogSequence()).To(BeZero(), "failure items should have log_sequence=0")
		}
	})

	It("Should have sequential order_index values", func() {
		// Submit 3 requests in a single batch
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "seq:a", big.NewInt(10), "USD"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "seq:b", big.NewInt(20), "USD"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "seq:c", big.NewInt(30), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		full, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(3))
		Expect(full.GetItems()[0].GetOrderIndex()).To(Equal(uint32(0)))
		Expect(full.GetItems()[1].GetOrderIndex()).To(Equal(uint32(1)))
		Expect(full.GetItems()[2].GetOrderIndex()).To(Equal(uint32(2)))
	})

	It("Should have item log_sequence that correlates to an actual log", func() {
		// Create a transaction to get a log sequence
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "logcorr:dest", big.NewInt(500), "USD"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.GetSuccess()).NotTo(BeNil())

		full, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(1))

		logSeq := full.GetItems()[0].GetLogSequence()
		Expect(logSeq).NotTo(BeZero())

		// Verify the log exists via GetLog
		log, err := actions.GetLog(sharedCtx, sharedClient, logSeq)
		Expect(err).To(Succeed())
		Expect(log).NotTo(BeNil())
		Expect(log.GetSequence()).To(Equal(logSeq))
	})

	It("Should have empty items on list entries", func() {
		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(entries).NotTo(BeEmpty())

		for _, entry := range entries {
			Expect(entry.GetItems()).To(BeEmpty(), "list entries should not have items populated")
			Expect(entry.GetOrderCount()).To(BeNumerically(">", 0), "order_count should be positive")
		}
	})

	It("Should have ledgers field populated on list entries when filtering by ledger", func() {
		filtered, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			Ledger: ledgerName,
		})
		Expect(err).To(Succeed())
		Expect(filtered).NotTo(BeEmpty())

		for _, entry := range filtered {
			Expect(entry.GetLedgers()).NotTo(BeEmpty(), "ledgers field should be populated on list entries")
		}
	})

	It("Should have multiple ledgers in a multi-ledger batch", func() {
		ledgerA := "audit-multi-a"
		ledgerB := "audit-multi-b"

		// Create 2 ledgers in one Apply
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateLedgerAction(ledgerA, nil),
				actions.CreateLedgerAction(ledgerB, nil),
			),
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		ledgers := last.GetLedgers()
		Expect(ledgers).To(ContainElement(ledgerA))
		Expect(ledgers).To(ContainElement(ledgerB))
	})

	It("Should return NOT_FOUND for non-existent audit entry", func() {
		_, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: 999999,
		})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})
})
