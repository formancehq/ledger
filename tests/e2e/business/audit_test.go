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

// auditStringFilter builds a single-condition string audit QueryFilter.
func auditStringFilter(field commonpb.AuditField, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Audit{
			Audit: &commonpb.AuditCondition{
				Field: field,
				Condition: &commonpb.AuditCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
					},
				},
			},
		},
	}
}

// filterReq wraps a QueryFilter in a ListAuditEntriesRequest.
func filterReq(filter *commonpb.QueryFilter) *servicepb.ListAuditEntriesRequest {
	return &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{Filter: filter}}
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
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
	})

	// Snapshot the current entry count before each test for relative assertions.
	BeforeEach(func() {
		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		entriesBeforeTest = len(entries)
	})

	It("Should record a success audit entry for a successful transaction", func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
		}, nil, nil)))
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
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("empty:account", "bank", big.NewInt(99999), "USD"),
		}, nil, nil)))
		Expect(err).To(HaveOccurred())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(entries)).To(Equal(entriesBeforeTest + 1))

		last := entries[len(entries)-1]
		Expect(last.GetFailure()).NotTo(BeNil(), "expected failure outcome")
		Expect(domain.ReasonString(last.GetFailure().GetReason())).To(Equal(domain.ErrReasonInsufficientFunds))
		Expect(last.GetFailure().Message).NotTo(BeEmpty())
	})

	It("Should filter audit entries by ledger name", func() {
		otherLedger := "audit-other-ledger"

		// Create a second ledger
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(otherLedger, nil)))
		Expect(err).To(Succeed())

		// Create a transaction on the second ledger
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(otherLedger, []*commonpb.Posting{
			actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
		}, nil, nil)))
		Expect(err).To(Succeed())

		// Filter by the original ledger via `audit[ledger] == <name>` — should not
		// include the second ledger's entries. The ledger scope is served by the
		// async audit index, so poll until it has caught up (Eventually).
		Eventually(func(g Gomega) {
			filtered, err := collectAuditEntries(sharedCtx, sharedClient,
				filterReq(auditStringFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, ledgerName)))
			g.Expect(err).To(Succeed())
			g.Expect(filtered).NotTo(BeEmpty())

			for _, entry := range filtered {
				g.Expect(entry.GetLedgers()).To(ContainElement(ledgerName),
					"filtered entry should target ledger %q", ledgerName)
			}
		}).Should(Succeed())

		// Filter by the other ledger — should include at least 2 entries (create + transaction)
		Eventually(func(g Gomega) {
			otherFiltered, err := collectAuditEntries(sharedCtx, sharedClient,
				filterReq(auditStringFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, otherLedger)))
			g.Expect(err).To(Succeed())
			g.Expect(len(otherFiltered)).To(BeNumerically(">=", 2))
		}).Should(Succeed())
	})

	It("Should filter audit entries by failures only", func() {
		// Create a successful transaction
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
		}, nil, nil)))
		Expect(err).To(Succeed())

		// Create a failing transaction
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("empty:account", "bank", big.NewInt(99999), "USD"),
		}, nil, nil)))
		Expect(err).To(HaveOccurred())

		// Get failures only via the shared filter — every returned entry must be
		// a failure. Served by the async audit index, so poll until caught up.
		Eventually(func(g Gomega) {
			failures, err := collectAuditEntries(sharedCtx, sharedClient,
				filterReq(auditStringFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")))
			g.Expect(err).To(Succeed())
			g.Expect(failures).NotTo(BeEmpty())
			for _, entry := range failures {
				g.Expect(entry.GetFailure()).NotTo(BeNil(), "expected only failure entries")
			}
		}).Should(Succeed())
	})

	It("Should support after_sequence pagination", func() {
		// Create a transaction to ensure we have entries
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
		}, nil, nil)))
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
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerForOrders, nil)))
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
		Expect(firstOrder.GetLedgerScoped()).NotTo(BeNil())
		Expect(firstOrder.GetLedgerScoped().GetLedger()).To(Equal(ledgerForOrders))
		Expect(firstOrder.GetLedgerScoped().GetCreateLedger()).NotTo(BeNil())

		// Create a transaction — produces an Apply/CreateTransaction order
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerForOrders, []*commonpb.Posting{
			actions.NewPosting("world", "bank", big.NewInt(500), "EUR"),
		}, nil, nil)))
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
		ls := decodeOrder(full.GetItems()[0]).GetLedgerScoped()
		Expect(ls).NotTo(BeNil())
		Expect(ls.GetLedger()).To(Equal(ledgerForOrders))
		apply := ls.GetApply()
		Expect(apply).NotTo(BeNil())
		Expect(apply.GetCreateTransaction()).NotTo(BeNil())
	})

	It("Should accept signed and unsigned batches and record audit entries", func() {
		// Create a fresh node for signing tests to avoid interfering with other tests
		sigCtx, sigClient, _ := testutil.SetupSingleNode(9109, 8109)

		// Generate a keypair
		pubKey, privKey, genErr := ed25519.GenerateKey(rand.Reader)
		Expect(genErr).To(Succeed())

		const keyID = "audit-test-key"

		// Register the key (bootstrap: first key can be unsigned)
		_, err := sigClient.Apply(sigCtx, servicepb.UnsignedApplyRequest("",
			actions.RegisterSigningKeyAction(keyID, pubKey),
		))
		Expect(err).To(Succeed())

		// Create a ledger with a signed batch
		signedReq := actions.CreateLedgerAction("signed-ledger", nil)
		signedLedger, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{signedReq}}, keyID, privKey)
		Expect(err).To(Succeed())
		_, err = sigClient.Apply(sigCtx, signedLedger)
		Expect(err).To(Succeed())

		// The batch signature is recorded once per proposal on AppliedProposal,
		// not on the Log, and AppliedProposal has no public read endpoint yet —
		// so the signature itself can't be asserted through the API here. What
		// is observable: admission verified the signed batch (the Apply above
		// succeeded) and both batches produced audit entries with their items.
		entries, err := collectAuditEntries(sigCtx, sigClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(entries)).To(BeNumerically(">=", 2))

		last := entries[len(entries)-1]
		full, err := sigClient.GetAuditEntry(sigCtx, &servicepb.GetAuditEntryRequest{
			Sequence: last.Sequence,
		})
		Expect(err).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(1))

		// The signed batch's log is still readable; it just no longer carries
		// the signature inline.
		signedLog, err := actions.GetLog(sigCtx, sigClient, full.GetItems()[0].GetLogSequence())
		Expect(err).To(Succeed())
		Expect(signedLog).NotTo(BeNil())
	})

	It("Should include multiple items in a batch audit entry", func() {
		// Submit multiple requests in a single Apply (batch)
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "batch:a", big.NewInt(100), "USD"),
		}, nil, nil),
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "batch:b", big.NewInt(200), "USD"),
			}, nil, nil)))
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
		Expect(decodeOrder(full.GetItems()[0]).GetLedgerScoped().GetApply()).NotTo(BeNil())
		Expect(decodeOrder(full.GetItems()[1]).GetLedgerScoped().GetApply()).NotTo(BeNil())
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
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("empty:nofunds", "bank", big.NewInt(99999), "USD"),
		}, nil, nil)))
		Expect(err).To(HaveOccurred())

		var last *auditpb.AuditEntry
		Eventually(func(g Gomega) {
			entries, err := collectAuditEntries(sharedCtx, sharedClient,
				filterReq(auditStringFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")))
			g.Expect(err).To(Succeed())
			g.Expect(entries).NotTo(BeEmpty())
			last = entries[len(entries)-1]
			g.Expect(last.GetFailure()).NotTo(BeNil())
		}).Should(Succeed())

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
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "seq:a", big.NewInt(10), "USD"),
		}, nil, nil),
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "seq:b", big.NewInt(20), "USD"),
			}, nil, nil),
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "seq:c", big.NewInt(30), "USD"),
			}, nil, nil)))
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
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "logcorr:dest", big.NewInt(500), "USD"),
		}, nil, nil)))
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
		Eventually(func(g Gomega) {
			filtered, err := collectAuditEntries(sharedCtx, sharedClient,
				filterReq(auditStringFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, ledgerName)))
			g.Expect(err).To(Succeed())
			g.Expect(filtered).NotTo(BeEmpty())

			for _, entry := range filtered {
				g.Expect(entry.GetLedgers()).NotTo(BeEmpty(), "ledgers field should be populated on list entries")
			}
		}).Should(Succeed())
	})

	It("Should have multiple ledgers in a multi-ledger batch", func() {
		ledgerA := "audit-multi-a"
		ledgerB := "audit-multi-b"

		// Create 2 ledgers in one Apply
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerA, nil),
			actions.CreateLedgerAction(ledgerB, nil)))
		Expect(err).To(Succeed())

		entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())

		last := entries[len(entries)-1]
		ledgers := last.GetLedgers()
		Expect(ledgers).To(ContainElement(ledgerA))
		Expect(ledgers).To(ContainElement(ledgerB))
	})

	It("Should honor options.filter for outcome == success (shared contract)", func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "filter:ok", big.NewInt(10), "USD"),
		}, nil, nil)))
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			entries, err := collectAuditEntries(sharedCtx, sharedClient,
				filterReq(auditStringFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success")))
			g.Expect(err).To(Succeed())
			g.Expect(entries).NotTo(BeEmpty())
			for _, entry := range entries {
				g.Expect(entry.GetSuccess()).NotTo(BeNil(), "expected only success entries")
			}
		}).Should(Succeed())
	})

	It("Should honor options.filter for order_type", func() {
		orderTypeLedger := "audit-order-type"
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(orderTypeLedger, nil)))
		Expect(err).To(Succeed())
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(orderTypeLedger, []*commonpb.Posting{
			actions.NewPosting("world", "ot:dest", big.NewInt(5), "USD"),
		}, nil, nil)))
		Expect(err).To(Succeed())

		// audit[ledger] == orderTypeLedger and audit[order_type] == create_transaction
		filter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
					auditStringFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, orderTypeLedger),
					auditStringFilter(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"),
				}},
			},
		}

		Eventually(func(g Gomega) {
			entries, err := collectAuditEntries(sharedCtx, sharedClient, filterReq(filter))
			g.Expect(err).To(Succeed())
			g.Expect(entries).NotTo(BeEmpty(), "expected the create_transaction audit entry")
			for _, entry := range entries {
				g.Expect(entry.GetLedgers()).To(ContainElement(orderTypeLedger))
			}
		}).Should(Succeed())
	})

	It("Should honor options.reverse (newest first)", func() {
		// Reverse iteration reads the audit zone directly (no filter), so it is
		// strongly consistent — no Eventually needed.
		asc, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			Options: &commonpb.ListOptions{Reverse: false},
		})
		Expect(err).To(Succeed())
		Expect(len(asc)).To(BeNumerically(">=", 2))

		desc, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
			Options: &commonpb.ListOptions{Reverse: true},
		})
		Expect(err).To(Succeed())
		Expect(len(desc)).To(Equal(len(asc)))

		// Ascending is oldest-first (increasing sequence); reverse is newest-first.
		Expect(asc[0].GetSequence()).To(BeNumerically("<", asc[len(asc)-1].GetSequence()))
		Expect(desc[0].GetSequence()).To(Equal(asc[len(asc)-1].GetSequence()))
		Expect(desc[len(desc)-1].GetSequence()).To(Equal(asc[0].GetSequence()))
	})

	It("Should honor options.filter for audit[seq] range", func() {
		all, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
		Expect(err).To(Succeed())
		Expect(len(all)).To(BeNumerically(">=", 3))

		lo := all[0].GetSequence()
		hi := all[len(all)-1].GetSequence() - 1
		seqFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Audit{
				Audit: &commonpb.AuditCondition{
					Field: commonpb.AuditField_AUDIT_FIELD_SEQUENCE,
					Condition: &commonpb.AuditCondition_UintCond{
						UintCond: &commonpb.UintCondition{Min: &lo, Max: &hi},
					},
				},
			},
		}

		ranged, err := collectAuditEntries(sharedCtx, sharedClient, filterReq(seqFilter))
		Expect(err).To(Succeed())
		for _, e := range ranged {
			Expect(e.GetSequence()).To(BeNumerically(">=", lo))
			Expect(e.GetSequence()).To(BeNumerically("<=", hi))
		}
	})

	It("Should reject an unsupported filter (not) with InvalidArgument", func() {
		notFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{
					Filter: auditStringFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				},
			},
		}

		_, err := collectAuditEntries(sharedCtx, sharedClient, filterReq(notFilter))
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("Should reject a non-audit filter condition with InvalidArgument", func() {
		metaFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "k"},
					Condition: &commonpb.FieldCondition_StringCond{
						StringCond: &commonpb.StringCondition{
							Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "v"},
						},
					},
				},
			},
		}

		_, err := collectAuditEntries(sharedCtx, sharedClient, filterReq(metaFilter))
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("Should return NOT_FOUND for non-existent audit entry", func() {
		_, err := sharedClient.GetAuditEntry(sharedCtx, &servicepb.GetAuditEntryRequest{
			Sequence: 999999,
		})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})
})
