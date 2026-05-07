//go:build e2e

package business

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// collectAuditEntries collects all audit entries from the streaming RPC.
func collectAuditEntries(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ListAuditEntriesRequest) ([]*auditpb.AuditEntry, error) {
	return actions.ListAuditEntriesWithRequest(ctx, client, req)
}

var _ = Describe("Audit Log", Ordered, func() {
	const ledgerName = "audit-test"

	// entriesBeforeTest tracks the number of audit entries before each test
	// so assertions can be relative to the test's own actions.
	var entriesBeforeTest int

	BeforeAll(func() {
		// Create the test ledger (generates 1 audit entry)
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
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
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			},
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
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("empty:account", "bank", big.NewInt(99999), "USD"),
				}, nil, nil),
			},
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
			Requests: []*servicepb.Request{actions.CreateLedgerAction(otherLedger, nil)},
		})
		Expect(err).To(Succeed())

		// Create a transaction on the second ledger
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(otherLedger, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		// Filter by the original ledger — should not include the second ledger's entries
		filtered, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			Ledger: ledgerName,
		})
		Expect(err).To(Succeed())

		for _, entry := range filtered {
			hasTargetLedger := false
			for _, order := range entry.GetOrders() {
				if apply := order.GetApply(); apply != nil && apply.GetLedger() == ledgerName {
					hasTargetLedger = true
				}
				if cl := order.GetCreateLedger(); cl != nil && cl.GetName() == ledgerName {
					hasTargetLedger = true
				}
			}
			Expect(hasTargetLedger).To(BeTrue(), "filtered entry should target ledger %q", ledgerName)
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
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		// Create a failing transaction
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("empty:account", "bank", big.NewInt(99999), "USD"),
				}, nil, nil),
			},
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
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		// Get all entries
		allEntries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(allEntries)).To(BeNumerically(">=", 2))

		// Get entries after the first one
		afterSeq := allEntries[0].Sequence
		afterEntries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			AfterSequence: &afterSeq,
		})
		Expect(err).To(Succeed())
		Expect(len(afterEntries)).To(Equal(len(allEntries) - 1))
	})

	It("Should include order details in audit entries", func() {
		// Create a ledger — produces a CreateLedger order
		ledgerForOrders := "audit-orders-test"
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerForOrders, nil)},
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.Orders).To(HaveLen(1))
		Expect(last.Orders[0].GetCreateLedger()).NotTo(BeNil())
		Expect(last.Orders[0].GetCreateLedger().Name).To(Equal(ledgerForOrders))

		// Create a transaction — produces an Apply/CreateTransaction order
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerForOrders, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(500), "EUR"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		entries, err = collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last = entries[len(entries)-1]
		Expect(last.Orders).To(HaveLen(1))
		apply := last.Orders[0].GetApply()
		Expect(apply).NotTo(BeNil())
		Expect(apply.Ledger).To(Equal(ledgerForOrders))
		Expect(apply.GetCreateTransaction()).NotTo(BeNil())
	})

	It("Should include signing key ID in audit entry orders", func() {
		// Create a fresh node for signing tests to avoid interfering with other tests
		sigCtx, sigClient, _ := testutil.SetupSingleNode(9109, 8109)

		// Generate a keypair
		pubKey, privKey, genErr := ed25519.GenerateKey(rand.Reader)
		Expect(genErr).To(Succeed())

		const keyID = "audit-test-key"

		// Register the key (bootstrap: first key can be unsigned)
		_, err := sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.RegisterSigningKeyAction(keyID, pubKey),
			},
		})
		Expect(err).To(Succeed())

		// Create a ledger with a signed request
		signedReq := actions.CreateLedgerAction("signed-ledger", nil)
		Expect(signing.Sign(signedReq, keyID, privKey)).To(Succeed())
		_, err = sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{signedReq},
		})
		Expect(err).To(Succeed())

		// Verify the audit entry contains the signing key
		entries, err := collectAuditEntries(sigCtx, sigClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.Orders).To(HaveLen(1))
		sig := last.Orders[0].GetSignature()
		Expect(sig).NotTo(BeNil())
		Expect(sig.KeyId).To(Equal(keyID))

		// Verify unsigned orders have no signature (entries[0] = RegisterSigningKey)
		Expect(len(entries)).To(BeNumerically(">=", 1))
		regEntry := entries[0]
		Expect(regEntry.Orders[0].GetSignature()).To(BeNil())
	})

	It("Should include multiple orders in a batch audit entry", func() {
		// Submit multiple requests in a single Apply (batch)
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "batch:a", big.NewInt(100), "USD"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "batch:b", big.NewInt(200), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.Orders).To(HaveLen(2))
		Expect(last.Orders[0].GetApply()).NotTo(BeNil())
		Expect(last.Orders[1].GetApply()).NotTo(BeNil())
	})

	It("Should get a single audit entry by sequence", func() {
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
		Expect(entry.Orders).To(HaveLen(len(target.Orders)))
	})

	It("Should return NOT_FOUND for non-existent audit entry", func() {
		_, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: 999999,
		})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})

})
