//go:build e2e

package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
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
	stream, err := client.ListAuditEntries(ctx, req)
	if err != nil {
		return nil, err
	}

	var entries []*auditpb.AuditEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

var _ = Describe("Audit Log", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort   = 9108
		grpcPort   = 8108
		ledgerName = "audit-test"
	)

	// entriesBeforeTest tracks the number of audit entries before each test
	// so assertions can be relative to the test's own actions.
	var entriesBeforeTest int

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)

		// Enable audit logging via RPC (audit starts disabled by default)
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				{
					Type: &servicepb.Request_SetAuditConfig{
						SetAuditConfig: &servicepb.SetAuditConfigRequest{
							Enabled: true,
						},
					},
				},
			},
		})
		Expect(err).To(Succeed())

		// Create the test ledger (generates 1 audit entry)
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
		})
		Expect(err).To(Succeed())
	})

	// Snapshot the current entry count before each test for relative assertions.
	BeforeEach(func() {
		entries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		entriesBeforeTest = len(entries)
	})

	It("Should record a success audit entry for a successful transaction", func() {
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(entries)).To(Equal(entriesBeforeTest + 1))

		last := entries[len(entries)-1]
		Expect(last.GetSuccess()).NotTo(BeNil(), "expected success outcome")
		Expect(last.GetSuccess().LogSequences).NotTo(BeEmpty())
		Expect(last.Sequence).NotTo(BeZero())
	})

	It("Should record a failure audit entry for insufficient funds", func() {
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("empty:account", "bank", big.NewInt(99999), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(HaveOccurred())

		entries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(entries)).To(Equal(entriesBeforeTest + 1))

		last := entries[len(entries)-1]
		Expect(last.GetFailure()).NotTo(BeNil(), "expected failure outcome")
		Expect(last.GetFailure().ErrorType).To(Equal(domain.ErrReasonInsufficientFunds))
		Expect(last.GetFailure().Message).NotTo(BeEmpty())
	})

	// TODO: "Should filter audit entries by ledger name" — requires adding a Ledger
	// field to ListAuditEntriesRequest in the proto definition.

	It("Should filter audit entries by failures only", func() {
		// Create a successful transaction
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		// Create a failing transaction
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("empty:account", "bank", big.NewInt(99999), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(HaveOccurred())

		// Get failures only — every returned entry must be a failure
		failures, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{
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
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "bank", big.NewInt(1000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		// Get all entries
		allEntries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(allEntries)).To(BeNumerically(">=", 2))

		// Get entries after the first one
		afterSeq := allEntries[0].Sequence
		afterEntries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{
			AfterSequence: &afterSeq,
		})
		Expect(err).To(Succeed())
		Expect(len(afterEntries)).To(Equal(len(allEntries) - 1))
	})

	It("Should include order details in audit entries", func() {
		// Create a ledger — produces a CreateLedger order
		ledgerForOrders := "audit-orders-test"
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{createLedgerAction(ledgerForOrders, nil)},
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.Orders).To(HaveLen(1))
		Expect(last.Orders[0].GetCreateLedger()).NotTo(BeNil())
		Expect(last.Orders[0].GetCreateLedger().Name).To(Equal(ledgerForOrders))

		// Create a transaction — produces an Apply/CreateTransaction order
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerForOrders, []*commonpb.Posting{
					newPosting("world", "bank", big.NewInt(500), "EUR"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		entries, err = collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
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
		sigCtx, sigClient, _ := setupSingleNode(9109, 8109)

		// Enable audit logging on the fresh node
		_, err := sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				{
					Type: &servicepb.Request_SetAuditConfig{
						SetAuditConfig: &servicepb.SetAuditConfigRequest{
							Enabled: true,
						},
					},
				},
			},
		})
		Expect(err).To(Succeed())

		// Generate a keypair
		pubKey, privKey, genErr := ed25519.GenerateKey(rand.Reader)
		Expect(genErr).To(Succeed())

		const keyID = "audit-test-key"

		// Register the key (bootstrap: first key can be unsigned)
		_, err = sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				registerSigningKeyAction(keyID, pubKey),
			},
		})
		Expect(err).To(Succeed())

		// Create a ledger with a signed request
		signedReq := createLedgerAction("signed-ledger", nil)
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

		// Verify unsigned orders have no signature (first entry after SetAuditConfig is RegisterSigningKey)
		// entries[0] = SetAuditConfig, entries[1] = RegisterSigningKey
		Expect(len(entries)).To(BeNumerically(">=", 2))
		regEntry := entries[1]
		Expect(regEntry.Orders[0].GetSignature()).To(BeNil())
	})

	It("Should include multiple orders in a batch audit entry", func() {
		// Submit multiple requests in a single Apply (batch)
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "batch:a", big.NewInt(100), "USD"),
				}, nil, nil),
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "batch:b", big.NewInt(200), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		Expect(last.Orders).To(HaveLen(2))
		Expect(last.Orders[0].GetApply()).NotTo(BeNil())
		Expect(last.Orders[1].GetApply()).NotTo(BeNil())
	})

	It("Should get a single audit entry by sequence", func() {
		// Get all entries first
		allEntries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(allEntries).NotTo(BeEmpty())

		// Get the first entry by sequence
		target := allEntries[0]
		entry, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
			Sequence: target.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(entry.Sequence).To(Equal(target.Sequence))
		Expect(entry.ProposalId).To(Equal(target.ProposalId))
		Expect(entry.Orders).To(HaveLen(len(target.Orders)))
	})

	It("Should return NOT_FOUND for non-existent audit entry", func() {
		_, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
			Sequence: 999999,
		})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})

	// The "FAILED_PRECONDITION when audit is disabled" test is covered in
	// audit_config_test.go which tests the full SetAuditConfig lifecycle.
})
