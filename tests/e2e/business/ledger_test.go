//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Ledger", Ordered, func() {

	Context("When saving account metadata via direct endpoint", Ordered, func() {
		var ledgerName = "test-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "test-account", big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		})

		It("Should save account metadata successfully", func() {
			metadata := map[string]string{
				"account_type": "asset",
				"label":        "Test Account",
			}

			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "test-account", metadata)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should merge metadata with existing account metadata", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "merge-account", big.NewInt(50), "USD"),
			}, nil, map[string]*commonpb.MetadataMap{
				"merge-account": commonpb.MetadataMapFromGoMap(map[string]string{
					"key1": "value1",
					"key2": "value2",
				}),
			})))
			Expect(err).To(Succeed())

			metadata := map[string]string{
				"key3": "value3",
				"key2": "updated_value2",
			}

			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "merge-account", metadata)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should delete account metadata successfully", func() {
			metadata := map[string]string{
				"to_delete": "value",
			}

			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "test-account", metadata)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			deleteResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.DeleteAccountMetadataAction(ledgerName, "test-account", "to_delete")))
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
			Expect(deleteResp.Logs).To(HaveLen(1))
		})
	})

	Context("When saving account metadata via bulk endpoint", Ordered, func() {
		var ledgerName = "bulk-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bulk-account", big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		})

		It("Should save account metadata via bulk endpoint", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{
				"account_type": "asset",
				"label":        "Bulk Account",
			})))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should handle multiple metadata operations in bulk", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bulk-account-2", big.NewInt(50), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{"key1": "value1"}),
				actions.SaveAccountMetadataAction(ledgerName, "bulk-account-2", map[string]string{"key2": "value2"})))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should delete account metadata via bulk endpoint", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{"to_delete": "value"}),
				actions.DeleteAccountMetadataAction(ledgerName, "bulk-account", "to_delete")))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})
	})

	Context("When creating ledgers", func() {
		var ledgerName = "test-ledger-create"

		It("Should create a ledger successfully", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed(), "Failed to create ledger")
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("Should return ALREADY_EXISTS with LEDGER_ALREADY_EXISTS reason when creating a duplicate ledger", func() {
			// Create ledger
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("dup-ledger", nil)))
			Expect(err).To(Succeed())

			// Try to create the same ledger again
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("dup-ledger", nil)))
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonLedgerAlreadyExists))
			Expect(info.Domain).To(Equal("ledger"))
			Expect(info.Metadata["name"]).To(Equal("dup-ledger"))
		})
	})

	Context("When saving transaction metadata via direct endpoint", Ordered, func() {
		var ledgerName = "transaction-metadata-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata successfully", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "transaction-metadata-account", big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			metadata := map[string]string{
				"reason": "adjustment",
				"source": "support",
			}

			saveResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveTransactionMetadataAction(ledgerName, transactionID, metadata)))
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(1))
		})

		It("Should delete transaction metadata successfully", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "transaction-metadata-account", big.NewInt(100), "USD"),
			}, map[string]string{"to_delete": "value"}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			deleteResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.DeleteTransactionMetadataAction(ledgerName, transactionID, "to_delete")))
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
			Expect(deleteResp.Logs).To(HaveLen(1))
		})
	})

	Context("When saving transaction metadata via bulk endpoint", Ordered, func() {
		var ledgerName = "transaction-metadata-bulk-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata via bulk endpoint", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "transaction-bulk-account", big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			saveResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
				"category": "bulk",
				"reason":   "reconciliation",
			})))
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(1))
		})

		It("Should delete transaction metadata via bulk endpoint", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "transaction-bulk-account", big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			saveResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveTransactionMetadataAction(ledgerName, transactionID, map[string]string{"to_delete": "value"}),
				actions.DeleteTransactionMetadataAction(ledgerName, transactionID, "to_delete")))
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(2))
		})
	})
})
