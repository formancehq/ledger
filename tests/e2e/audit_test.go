//go:build e2e

package e2e

import (
	"context"
	"io"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
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

		// Create the test ledger (generates 1 audit entry)
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
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
		Expect(last.GetFailure().ErrorType).To(Equal(processing.ErrReasonInsufficientFunds))
		Expect(last.GetFailure().Message).NotTo(BeEmpty())
	})

	It("Should filter audit entries by ledger name", func() {
		// Create a second ledger and a transaction on the main ledger
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{createLedgerAction("other-ledger", nil)},
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "bank", big.NewInt(500), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		// Filter by audit-test ledger — should exclude "other-ledger" entries
		filtered, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{
			Ledger: ledgerName,
		})
		Expect(err).To(Succeed())
		Expect(filtered).NotTo(BeEmpty())

		all, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(filtered)).To(BeNumerically("<", len(all)), "filtered should have fewer entries than all")
	})

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
		afterEntries, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{
			AfterSequence: allEntries[0].Sequence,
		})
		Expect(err).To(Succeed())
		Expect(len(afterEntries)).To(Equal(len(allEntries) - 1))
	})

	It("Should return FAILED_PRECONDITION when audit is disabled", func() {
		// The cluster was started with audit enabled (default).
		// A full test would require a separate node with --audit-enabled=false.
		Skip("Requires separate node setup with audit disabled")

		_, err := collectAuditEntries(ctx, client, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(HaveOccurred())

		st, ok := status.FromError(err)
		Expect(ok).To(BeTrue())
		Expect(st.Code()).To(Equal(codes.FailedPrecondition))

		info := extractGRPCErrorInfo(err)
		Expect(info).NotTo(BeNil())
		Expect(info.Reason).To(Equal(processing.ErrReasonAuditDisabled))
		Expect(info.Domain).To(Equal("ledger"))
	})
})
