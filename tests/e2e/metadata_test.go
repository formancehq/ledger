//go:build e2e

package e2e

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metadata", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("Account Metadata", Ordered, func() {
		var ledgerName = "account-metadata-ledger"

		BeforeAll(func() {
			// Create ledger
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create account via transaction
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "test-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", metadata)},
			})
			Expect(err).To(Succeed())

			// Verify metadata via GetAccount
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Metadata).NotTo(BeNil())
			metaMap := account.Metadata.ToMap()
			Expect(metaMap["type"]).To(Equal("savings"))
			Expect(metaMap["owner"]).To(Equal("user123"))
			Expect(metaMap["tier"]).To(Equal("premium"))
		})

		It("Should update existing metadata", func() {
			// Set initial metadata
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"key1": "value1",
					"key2": "value2",
				})},
			})
			Expect(err).To(Succeed())

			// Update metadata (should merge)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"key2": "updated_value2",
					"key3": "value3",
				})},
			})
			Expect(err).To(Succeed())

			// Verify merged metadata
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := account.Metadata.ToMap()
			Expect(metaMap["key1"]).To(Equal("value1"))
			Expect(metaMap["key2"]).To(Equal("updated_value2"))
			Expect(metaMap["key3"]).To(Equal("value3"))
		})

		It("Should delete metadata and verify removal", func() {
			// Set metadata
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"keep":   "this",
					"delete": "this",
				})},
			})
			Expect(err).To(Succeed())

			// Delete one key
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteAccountMetadataAction(ledgerName, "test-account", "delete")},
			})
			Expect(err).To(Succeed())

			// Verify deletion (deleted keys should not exist)
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := account.Metadata.ToMap()
			Expect(metaMap["keep"]).To(Equal("this"))
			// Deleted metadata key should not exist in the map
			_, exists := metaMap["delete"]
			Expect(exists).To(BeFalse())
		})

		It("Should set metadata on account that doesn't have transactions yet", func() {
			// Set metadata on a new account (creates account implicitly)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "new-account", map[string]string{
					"created": "via-metadata",
				})},
			})
			Expect(err).To(Succeed())

			// Verify account exists with metadata
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "new-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Metadata.ToMap()["created"]).To(Equal("via-metadata"))
		})

		It("Should handle metadata with special characters", func() {
			// Set metadata with special characters
			metadata := map[string]string{
				"url":         "https://example.com/path?query=value",
				"description": "A \"quoted\" string with 'apostrophes'",
				"json":        `{"key": "value"}`,
			}
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", metadata)},
			})
			Expect(err).To(Succeed())

			// Verify
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := account.Metadata.ToMap()
			Expect(metaMap["url"]).To(Equal("https://example.com/path?query=value"))
			Expect(metaMap["description"]).To(Equal("A \"quoted\" string with 'apostrophes'"))
			Expect(metaMap["json"]).To(Equal(`{"key": "value"}`))
		})

		It("Should delete multiple metadata keys sequentially", func() {
			// Set multiple metadata keys
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				})},
			})
			Expect(err).To(Succeed())

			// Delete keys one by one
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteAccountMetadataAction(ledgerName, "test-account", "key1")},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteAccountMetadataAction(ledgerName, "test-account", "key2")},
			})
			Expect(err).To(Succeed())

			// Verify: key1 and key2 should not exist, key3 should remain
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "test-account",
			})
			Expect(err).To(Succeed())
			metaMap := account.Metadata.ToMap()
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should be able to get a transaction after creating it", func() {
			// Create transaction without metadata
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).To(Equal(uint64(1)))

			// Get the transaction immediately
			tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(tx.Id).To(Equal(transactionID))
		})

		It("Should be able to get a transaction before and after saving metadata", func() {
			// Create transaction without metadata
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Get the transaction BEFORE saving metadata - should work
			txBefore, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed(), "GetTransaction before metadata should succeed")
			Expect(txBefore.Id).To(Equal(transactionID))

			// Save metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"key": "value",
				})},
			})
			Expect(err).To(Succeed(), "Save metadata should succeed")

			// Get the transaction AFTER saving metadata - this is what fails
			txAfter, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed(), "GetTransaction after metadata should succeed")
			Expect(txAfter.Id).To(Equal(transactionID))
			Expect(txAfter.Metadata.ToMap()["key"]).To(Equal("value"))
		})

		It("Should set metadata and verify it persists", func() {
			// Create transaction
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
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
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, transactionID, metadata)},
			})
			Expect(err).To(Succeed())

			// Verify metadata via GetTransaction
			tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(tx.Metadata).NotTo(BeNil())
			metaMap := tx.Metadata.ToMap()
			Expect(metaMap["reference"]).To(Equal("order-123"))
			Expect(metaMap["source"]).To(Equal("api"))
			Expect(metaMap["status"]).To(Equal("processed"))
		})

		It("Should update existing transaction metadata", func() {
			// Create transaction
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Set initial metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"status": "pending",
					"key1":   "value1",
				})},
			})
			Expect(err).To(Succeed())

			// Update metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"status": "completed",
					"key2":   "value2",
				})},
			})
			Expect(err).To(Succeed())

			// Verify merged metadata
			tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			metaMap := tx.Metadata.ToMap()
			Expect(metaMap["status"]).To(Equal("completed"))
			Expect(metaMap["key1"]).To(Equal("value1"))
			Expect(metaMap["key2"]).To(Equal("value2"))
		})

		It("Should delete transaction metadata and verify removal", func() {
			// Create transaction
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Set metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"keep":   "this",
					"delete": "this",
				})},
			})
			Expect(err).To(Succeed())

			// Delete one key
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteTransactionMetadataAction(ledgerName, transactionID, "delete")},
			})
			Expect(err).To(Succeed())

			// Verify deletion (deleted keys should not exist)
			tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			metaMap := tx.Metadata.ToMap()
			Expect(metaMap["keep"]).To(Equal("this"))
			// Deleted metadata key should not exist in the map
			_, exists := metaMap["delete"]
			Expect(exists).To(BeFalse())
		})

		It("Should preserve metadata set at transaction creation", func() {
			// Create transaction with initial metadata
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user", big.NewInt(500), "USD"),
					}, map[string]string{
						"initial": "metadata",
						"type":    "deposit",
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			newTxID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Add more metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, newTxID, map[string]string{
					"additional": "metadata",
				})},
			})
			Expect(err).To(Succeed())

			// Verify all metadata is present
			tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: newTxID,
			})
			Expect(err).To(Succeed())
			metaMap := tx.Metadata.ToMap()
			Expect(metaMap["initial"]).To(Equal("metadata"))
			Expect(metaMap["type"]).To(Equal("deposit"))
			Expect(metaMap["additional"]).To(Equal("metadata"))
		})

		It("Should delete multiple metadata keys in bulk", func() {
			// Create transaction
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Set multiple metadata keys
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
					"key4": "value4",
				})},
			})
			Expect(err).To(Succeed())

			// Delete multiple keys in one bulk request
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					deleteTransactionMetadataAction(ledgerName, transactionID, "key1"),
					deleteTransactionMetadataAction(ledgerName, transactionID, "key3"),
				},
			})
			Expect(err).To(Succeed())

			// Verify: key1 and key3 should not exist, key2 and key4 should remain
			tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			metaMap := tx.Metadata.ToMap()
			_, key1Exists := metaMap["key1"]
			Expect(key1Exists).To(BeFalse())
			Expect(metaMap["key2"]).To(Equal("value2"))
			_, key3Exists := metaMap["key3"]
			Expect(key3Exists).To(BeFalse())
			Expect(metaMap["key4"]).To(Equal("value4"))
		})

		It("Should handle metadata on reverted transaction", func() {
			// Create transaction to revert
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user", big.NewInt(100), "USD"),
					}, map[string]string{"original": "true"}, nil),
				},
			})
			Expect(err).To(Succeed())

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			originalTxID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction
			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, originalTxID, false, false, map[string]string{
					"revert_reason": "test",
				})},
			})
			Expect(err).To(Succeed())

			// Get the revert transaction ID
			revertLog := revertResp.Logs[0]
			revertApplyLog := revertLog.Payload.GetApply()
			revertTxID := revertApplyLog.Log.Data.GetRevertedTransaction().RevertTransaction.Id

			// Verify metadata on revert transaction
			revertTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: revertTxID,
			})
			Expect(err).To(Succeed())
			Expect(revertTx.Metadata.ToMap()["revert_reason"]).To(Equal("test"))

			// Original transaction should still have its metadata and be marked as reverted
			originalTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: originalTxID,
			})
			Expect(err).To(Succeed())
			Expect(originalTx.Reverted).To(BeTrue())
			Expect(originalTx.Metadata.ToMap()["original"]).To(Equal("true"))
		})
	})
})
