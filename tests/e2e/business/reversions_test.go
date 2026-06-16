//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Reversions", Ordered, func() {

	Context("When reverting transactions", Ordered, func() {
		var ledgerName = "revert-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should revert a transaction successfully", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
			Expect(createResp.Logs).To(HaveLen(1))

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			// Revert the transaction
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should revert a transaction with metadata", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with metadata
			revertMetadata := map[string]string{
				"reason": "correction",
				"source": "support",
			}

			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, revertMetadata)),
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))

			// Verify the reverting transaction has the metadata
			revertLog := revertResp.Logs[0]
			revertApplyLog := revertLog.Payload.GetApply()
			revertTx := revertApplyLog.Log.Data.GetRevertedTransaction()
			Expect(revertTx).NotTo(BeNil())
			Expect(revertTx.RevertTransaction.Metadata).NotTo(BeNil())
			Expect(commonpb.MetadataToGoMap(revertTx.RevertTransaction.Metadata)["reason"]).To(Equal("correction"))
			Expect(commonpb.MetadataToGoMap(revertTx.RevertTransaction.Metadata)["source"]).To(Equal("support"))
		})

		It("Should revert a transaction with force flag", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with force flag
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, true, false, nil)),
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should revert a transaction with atEffectiveDate flag", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with atEffectiveDate flag
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, true, nil)),
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should fail to revert a non-existent transaction", func() {
			nonExistentTransactionID := uint64(99999)

			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, nonExistentTransactionID, false, false, nil)),
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionNotFound))
			Expect(info.Domain).To(Equal("ledger"))
		})

		It("Should fail to revert an already reverted transaction", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction first time
			revertResp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(Succeed())
			Expect(revertResp1).NotTo(BeNil())

			// Try to revert the same transaction again
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionAlreadyReverted))
			Expect(info.Domain).To(Equal("ledger"))
		})

		It("Should revert multiple transactions in bulk", func() {
			// Create multiple transactions
			var transactionIDs []uint64
			for i := 0; i < 3; i++ {
				createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Envelopes: servicepb.UnsignedEnvelopes(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("account-%d", i+1), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					),
				})
				Expect(err).To(Succeed())
				log := createResp.Logs[0]
				applyLog := log.Payload.GetApply()
				transactionIDs = append(transactionIDs, applyLog.Log.Data.GetCreatedTransaction().Transaction.Id)
			}

			// Revert all transactions in bulk
			revertReqs := make([]*servicepb.Request, len(transactionIDs))
			for i, txID := range transactionIDs {
				revertReqs[i] = actions.RevertTransactionAction(ledgerName, txID, false, false, nil)
			}

			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(revertReqs...),
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(len(transactionIDs)))
		})
	})

	Context("When verifying balance restoration after revert", Ordered, func() {
		var ledgerName = "revert-balance-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should restore account balances after revert", func() {
			// Create a transaction: world -> account-1 (100 USD)
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Verify initial balance
			account1, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1.Volumes["USD"].Balance).To(Equal("100"))

			// Revert the transaction
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(Succeed())

			// Verify balance is restored (should be 0)
			account1After, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1After.Volumes["USD"].Balance).To(Equal("0"))
		})

		It("Should restore balances for multi-posting transaction", func() {
			// Create a transaction with multiple postings
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-a", big.NewInt(100), "USD"),
						actions.NewPosting("world", "account-b", big.NewInt(200), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Verify initial balances
			accountA, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-a",
			})
			Expect(err).To(Succeed())
			Expect(accountA.Volumes["USD"].Balance).To(Equal("100"))

			accountB, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-b",
			})
			Expect(err).To(Succeed())
			Expect(accountB.Volumes["USD"].Balance).To(Equal("200"))

			// Revert the transaction
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(Succeed())

			// Verify balances are restored
			accountAAfter, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-a",
			})
			Expect(err).To(Succeed())
			Expect(accountAAfter.Volumes["USD"].Balance).To(Equal("0"))

			accountBAfter, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-b",
			})
			Expect(err).To(Succeed())
			Expect(accountBAfter.Volumes["USD"].Balance).To(Equal("0"))
		})

		It("Should correctly track volumes after revert", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "volume-account", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Revert the transaction
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(Succeed())

			// Verify volumes: input=100, output=100, balance=0
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "volume-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Input).To(Equal("100"))
			Expect(account.Volumes["USD"].Output).To(Equal("100"))
			Expect(account.Volumes["USD"].Balance).To(Equal("0"))
		})
	})

	Context("When verifying reverted transaction status", Ordered, func() {
		var ledgerName = "revert-status-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should mark original transaction as reverted", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Verify transaction is not reverted initially
			tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(tx.Transaction.Reverted).To(BeFalse())

			// Revert the transaction
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(Succeed())

			// Verify transaction is now marked as reverted
			txAfter, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(txAfter.Transaction.Reverted).To(BeTrue())
		})

		It("Should create a new reverting transaction", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			originalTxID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, originalTxID, false, false, nil)),
			})
			Expect(err).To(Succeed())

			// Get the reverting transaction from the reverted log
			revertLog := revertResp.Logs[0]
			revertApplyLog := revertLog.Payload.GetApply()
			revertedTx := revertApplyLog.Log.Data.GetRevertedTransaction()
			Expect(revertedTx).NotTo(BeNil())
			revertTxID := revertedTx.RevertTransaction.Id

			// Verify it's a different transaction
			Expect(revertTxID).NotTo(Equal(originalTxID))

			// Verify the reverting transaction has opposite postings
			Expect(revertedTx.RevertTransaction.Postings).To(HaveLen(1))
			Expect(revertedTx.RevertTransaction.Postings[0].Source).To(Equal("account-1"))
			Expect(revertedTx.RevertTransaction.Postings[0].Destination).To(Equal("world"))
		})
	})

	Context("When reverting with insufficient funds", Ordered, func() {
		var ledgerName = "revert-insufficient-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should fail to revert when account has insufficient funds without force flag", func() {
			// Create a transaction: world -> account-1 (100 USD)
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Spend the funds: account-1 -> account-2 (100 USD)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("account-1", "account-2", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Try to revert the original transaction (account-1 has 0 balance)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, false, false, nil)),
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("insufficient"))
		})

		It("Should succeed to revert when account has insufficient funds with force flag", func() {
			// Create a transaction: world -> account-1 (100 USD)
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Spend the funds: account-1 -> account-2 (100 USD)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("account-1", "account-2", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Revert with force flag (should succeed even with negative balance)
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, transactionID, true, false, nil)),
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))

			// Verify account-1 has negative balance
			account1, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1.Volumes["USD"].Balance).To(Equal("-100"))
		})
	})

	Context("When reverting on invalid ledger", func() {
		It("Should fail to revert on non-existent ledger", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction("non-existent-ledger", 1, false, false, nil)),
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When reverting transactions with expandVolumes", Ordered, func() {
		var ledgerName = "revert-expand-volumes-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should not include postCommitVolumes on revert when expandVolumes is false", func() {
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-rv-no-expand", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			txID := createResp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.RevertTransactionAction(ledgerName, txID, false, false, nil),
				),
			})
			Expect(err).To(Succeed())

			revertedTx := revertResp.Logs[0].Payload.GetApply().Log.Data.GetRevertedTransaction()
			Expect(revertedTx.PostCommitVolumes).To(BeNil())
		})

		It("Should include postCommitVolumes on revert when expandVolumes is true", func() {
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-rv-expand", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			txID := createResp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.WithExpandVolumes(actions.RevertTransactionAction(ledgerName, txID, false, false, nil)),
				),
			})
			Expect(err).To(Succeed())

			revertedTx := revertResp.Logs[0].Payload.GetApply().Log.Data.GetRevertedTransaction()
			Expect(revertedTx.PostCommitVolumes).NotTo(BeNil())

			pcv := revertedTx.PostCommitVolumes.VolumesByAccount
			// After revert: ev-rv-expand sent 100 back to world -> input=100, output=100
			Expect(pcv).To(HaveKey("ev-rv-expand"))
			Expect(pcv["ev-rv-expand"].Volumes["USD"].Input).To(Equal("100"))
			Expect(pcv["ev-rv-expand"].Volumes["USD"].Output).To(Equal("100"))

			Expect(pcv).To(HaveKey("world"))
		})

		It("Should include correct postCommitVolumes on force revert with spent funds", func() {
			// Create: world -> ev-rv-force (100 USD)
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-rv-force", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			txID := createResp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Spend the funds: ev-rv-force -> other (100 USD)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("ev-rv-force", "ev-rv-force-other", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Force revert with expandVolumes
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.WithExpandVolumes(actions.RevertTransactionAction(ledgerName, txID, true, false, nil)),
				),
			})
			Expect(err).To(Succeed())

			revertedTx := revertResp.Logs[0].Payload.GetApply().Log.Data.GetRevertedTransaction()
			Expect(revertedTx.PostCommitVolumes).NotTo(BeNil())

			pcv := revertedTx.PostCommitVolumes.VolumesByAccount
			// ev-rv-force: input=100 (original), output=200 (100 spent + 100 reverted)
			Expect(pcv).To(HaveKey("ev-rv-force"))
			Expect(pcv["ev-rv-force"].Volumes["USD"].Input).To(Equal("100"))
			Expect(pcv["ev-rv-force"].Volumes["USD"].Output).To(Equal("200"))
		})
	})
})
