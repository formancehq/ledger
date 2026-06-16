//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metadata", Ordered, func() {

	Context("Account Metadata", Ordered, func() {
		var ledgerName = "account-metadata-ledger"

		BeforeAll(func() {
			// Create ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())

			// Create account via transaction
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "test-account", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
		})

		It("Should set metadata and verify it persists", func() {
			// Set metadata
			metadata := map[string]string{
				"type":  "savings",
				"owner": "user123",
				"tier":  "premium",
			}
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveAccountMetadataAction(ledgerName, "test-account", metadata)),
			})
			Expect(err).To(Succeed())

			// Verify metadata via GetAccount
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Metadata).NotTo(BeNil())
			metaMap := commonpb.MetadataToGoMap(account.Metadata)
			Expect(metaMap["type"]).To(Equal("savings"))
			Expect(metaMap["owner"]).To(Equal("user123"))
			Expect(metaMap["tier"]).To(Equal("premium"))
		})

		It("Should update existing metadata", func() {
			// Set initial metadata
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"key1": "value1",
					"key2": "value2",
				})),
			})
			Expect(err).To(Succeed())

			// Update metadata (should merge)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"key2": "updated_value2",
					"key3": "value3",
				})),
			})
			Expect(err).To(Succeed())

			// Verify merged metadata
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(account.Metadata)
			Expect(metaMap["key1"]).To(Equal("value1"))
			Expect(metaMap["key2"]).To(Equal("updated_value2"))
			Expect(metaMap["key3"]).To(Equal("value3"))
		})

		It("Should delete metadata and verify removal", func() {
			// Set metadata
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"keep":   "this",
					"delete": "this",
				})),
			})
			Expect(err).To(Succeed())

			// Delete one key
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.DeleteAccountMetadataAction(ledgerName, "test-account", "delete")),
			})
			Expect(err).To(Succeed())

			// Verify deletion (deleted keys should not exist)
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(account.Metadata)
			Expect(metaMap["keep"]).To(Equal("this"))
			// Deleted metadata key should not exist in the map
			_, exists := metaMap["delete"]
			Expect(exists).To(BeFalse())
		})

		It("Should set metadata on account that doesn't have transactions yet", func() {
			// Set metadata on a new account (creates account implicitly)
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveAccountMetadataAction(ledgerName, "new-account", map[string]string{
					"created": "via-metadata",
				})),
			})
			Expect(err).To(Succeed())

			// Verify account exists with metadata
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "new-account",
			})
			Expect(err).To(Succeed())
			Expect(commonpb.MetadataToGoMap(account.Metadata)["created"]).To(Equal("via-metadata"))
		})

		It("Should handle metadata with special characters", func() {
			// Set metadata with special characters
			metadata := map[string]string{
				"url":         "https://example.com/path?query=value",
				"description": "A \"quoted\" string with 'apostrophes'",
				"json":        `{"key": "value"}`,
			}
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveAccountMetadataAction(ledgerName, "test-account", metadata)),
			})
			Expect(err).To(Succeed())

			// Verify
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(account.Metadata)
			Expect(metaMap["url"]).To(Equal("https://example.com/path?query=value"))
			Expect(metaMap["description"]).To(Equal("A \"quoted\" string with 'apostrophes'"))
			Expect(metaMap["json"]).To(Equal(`{"key": "value"}`))
		})

		It("Should delete multiple metadata keys sequentially", func() {
			// Set multiple metadata keys
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				})),
			})
			Expect(err).To(Succeed())

			// Delete keys one by one
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.DeleteAccountMetadataAction(ledgerName, "test-account", "key1")),
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.DeleteAccountMetadataAction(ledgerName, "test-account", "key2")),
			})
			Expect(err).To(Succeed())

			// Verify: key1 and key2 should not exist, key3 should remain
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(account.Metadata)
			_, key1Exists := metaMap["key1"]
			Expect(key1Exists).To(BeFalse())
			_, key2Exists := metaMap["key2"]
			Expect(key2Exists).To(BeFalse())
			Expect(metaMap["key3"]).To(Equal("value3"))
		})
	})

	Context("Transaction Metadata", Ordered, func() {
		var ledgerName = "tx-metadata-ledger"

		BeforeAll(func() {
			// Create ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should be able to get a transaction after creating it", func() {
			// Create transaction without metadata
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).To(Equal(uint64(1)))

			// Get the transaction immediately
			tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(tx.Transaction.Id).To(Equal(transactionID))
		})

		It("Should be able to get a transaction before and after saving metadata", func() {
			// Create transaction without metadata
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Get the transaction BEFORE saving metadata - should work
			txBefore, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed(), "GetTransaction before metadata should succeed")
			Expect(txBefore.Transaction.Id).To(Equal(transactionID))

			// Save metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"key": "value",
				})),
			})
			Expect(err).To(Succeed(), "Save metadata should succeed")

			// Get the transaction AFTER saving metadata - this is what fails
			txAfter, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed(), "GetTransaction after metadata should succeed")
			Expect(txAfter.Transaction.Id).To(Equal(transactionID))
			Expect(commonpb.MetadataToGoMap(txAfter.Transaction.Metadata)["key"]).To(Equal("value"))
		})

		It("Should set metadata and verify it persists", func() {
			// Create transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Set metadata
			metadata := map[string]string{
				"reference": "order-123",
				"source":    "api",
				"status":    "processed",
			}
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveTransactionMetadataAction(ledgerName, transactionID, metadata)),
			})
			Expect(err).To(Succeed())

			// Verify metadata via GetTransaction
			tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(tx.Transaction.Metadata).NotTo(BeNil())
			metaMap := commonpb.MetadataToGoMap(tx.Transaction.Metadata)
			Expect(metaMap["reference"]).To(Equal("order-123"))
			Expect(metaMap["source"]).To(Equal("api"))
			Expect(metaMap["status"]).To(Equal("processed"))
		})

		It("Should update existing transaction metadata", func() {
			// Create transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Set initial metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"status": "pending",
					"key1":   "value1",
				})),
			})
			Expect(err).To(Succeed())

			// Update metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"status": "completed",
					"key2":   "value2",
				})),
			})
			Expect(err).To(Succeed())

			// Verify merged metadata
			tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(tx.Transaction.Metadata)
			Expect(metaMap["status"]).To(Equal("completed"))
			Expect(metaMap["key1"]).To(Equal("value1"))
			Expect(metaMap["key2"]).To(Equal("value2"))
		})

		It("Should delete transaction metadata and verify removal", func() {
			// Create transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Set metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"keep":   "this",
					"delete": "this",
				})),
			})
			Expect(err).To(Succeed())

			// Delete one key
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.DeleteTransactionMetadataAction(ledgerName, transactionID, "delete")),
			})
			Expect(err).To(Succeed())

			// Verify deletion (deleted keys should not exist)
			tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(tx.Transaction.Metadata)
			Expect(metaMap["keep"]).To(Equal("this"))
			// Deleted metadata key should not exist in the map
			_, exists := metaMap["delete"]
			Expect(exists).To(BeFalse())
		})

		It("Should preserve metadata set at transaction creation", func() {
			// Create transaction with initial metadata
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user", big.NewInt(500), "USD"),
					}, map[string]string{
						"initial": "metadata",
						"type":    "deposit",
					}, nil),
				),
			})
			Expect(err).To(Succeed())

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			newTxID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Add more metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveTransactionMetadataAction(ledgerName, newTxID, map[string]string{
					"additional": "metadata",
				})),
			})
			Expect(err).To(Succeed())

			// Verify all metadata is present
			tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: newTxID,
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(tx.Transaction.Metadata)
			Expect(metaMap["initial"]).To(Equal("metadata"))
			Expect(metaMap["type"]).To(Equal("deposit"))
			Expect(metaMap["additional"]).To(Equal("metadata"))
		})

		It("Should delete multiple metadata keys in bulk", func() {
			// Create transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Set multiple metadata keys
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.SaveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
					"key4": "value4",
				})),
			})
			Expect(err).To(Succeed())

			// Delete multiple keys in one bulk request
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.DeleteTransactionMetadataAction(ledgerName, transactionID, "key1"),
					actions.DeleteTransactionMetadataAction(ledgerName, transactionID, "key3"),
				),
			})
			Expect(err).To(Succeed())

			// Verify: key1 and key3 should not exist, key2 and key4 should remain
			tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(tx.Transaction.Metadata)
			_, key1Exists := metaMap["key1"]
			Expect(key1Exists).To(BeFalse())
			Expect(metaMap["key2"]).To(Equal("value2"))
			_, key3Exists := metaMap["key3"]
			Expect(key3Exists).To(BeFalse())
			Expect(metaMap["key4"]).To(Equal("value4"))
		})

		It("Should handle metadata on reverted transaction", func() {
			// Create transaction to revert
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user", big.NewInt(100), "USD"),
					}, map[string]string{"original": "true"}, nil),
				),
			})
			Expect(err).To(Succeed())

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			originalTxID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, originalTxID, false, false, map[string]string{
					"revert_reason": "test",
				})),
			})
			Expect(err).To(Succeed())

			// Get the revert transaction ID
			revertLog := revertResp.Logs[0]
			revertApplyLog := revertLog.Payload.GetApply()
			revertTxID := revertApplyLog.Log.Data.GetRevertedTransaction().RevertTransaction.Id

			// Verify metadata on revert transaction
			revertTx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: revertTxID,
			})
			Expect(err).To(Succeed())
			Expect(commonpb.MetadataToGoMap(revertTx.Transaction.Metadata)["revert_reason"]).To(Equal("test"))

			// Original transaction should still have its metadata and be marked as reverted
			originalTx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: originalTxID,
			})
			Expect(err).To(Succeed())
			Expect(originalTx.Transaction.Reverted).To(BeTrue())
			Expect(commonpb.MetadataToGoMap(originalTx.Transaction.Metadata)["original"]).To(Equal("true"))
		})
	})
})
