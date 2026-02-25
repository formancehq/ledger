//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Reversions", Ordered, func() {
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

	Context("When reverting transactions", Ordered, func() {
		var ledgerName = "revert-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should revert a transaction successfully", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp.Logs).To(HaveLen(1))

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			// Revert the transaction
			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should revert a transaction with metadata", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
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

			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, revertMetadata)},
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
			Expect(revertTx.RevertTransaction.Metadata.ToMap()["reason"]).To(Equal("correction"))
			Expect(revertTx.RevertTransaction.Metadata.ToMap()["source"]).To(Equal("support"))
		})

		It("Should revert a transaction with force flag", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with force flag
			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, true, false, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should revert a transaction with atEffectiveDate flag", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with atEffectiveDate flag
			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, true, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should fail to revert a non-existent transaction", func() {
			nonExistentTransactionID := uint64(99999)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, nonExistentTransactionID, false, false, nil)},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionNotFound))
			Expect(info.Domain).To(Equal("ledger"))
		})

		It("Should fail to revert an already reverted transaction", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction first time
			revertResp1, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp1).NotTo(BeNil())

			// Try to revert the same transaction again
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionAlreadyReverted))
			Expect(info.Domain).To(Equal("ledger"))
		})

		It("Should revert multiple transactions in bulk", func() {
			// Create multiple transactions
			var transactionIDs []uint64
			for i := 0; i < 3; i++ {
				createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("account-%d", i+1), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
				log := createResp.Logs[0]
				applyLog := log.Payload.GetApply()
				transactionIDs = append(transactionIDs, applyLog.Log.Data.GetCreatedTransaction().Transaction.Id)
			}

			// Revert all transactions in bulk
			actions := make([]*servicepb.Request, len(transactionIDs))
			for i, txID := range transactionIDs {
				actions[i] = revertTransactionAction(ledgerName, txID, false, false, nil)
			}

			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: actions,
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(len(transactionIDs)))
		})
	})

	Context("When verifying balance restoration after revert", Ordered, func() {
		var ledgerName = "revert-balance-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should restore account balances after revert", func() {
			// Create a transaction: world -> account-1 (100 USD)
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify initial balance
			account1, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1.Volumes["USD"].Balance).To(Equal("100"))

			// Revert the transaction
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())

			// Verify balance is restored (should be 0)
			account1After, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1After.Volumes["USD"].Balance).To(Equal("0"))
		})

		It("Should restore balances for multi-posting transaction", func() {
			// Create a transaction with multiple postings
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-a", big.NewInt(100), "USD"),
						newPosting("world", "account-b", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify initial balances
			accountA, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-a",
			})
			Expect(err).To(Succeed())
			Expect(accountA.Volumes["USD"].Balance).To(Equal("100"))

			accountB, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-b",
			})
			Expect(err).To(Succeed())
			Expect(accountB.Volumes["USD"].Balance).To(Equal("200"))

			// Revert the transaction
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())

			// Verify balances are restored
			accountAAfter, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-a",
			})
			Expect(err).To(Succeed())
			Expect(accountAAfter.Volumes["USD"].Balance).To(Equal("0"))

			accountBAfter, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-b",
			})
			Expect(err).To(Succeed())
			Expect(accountBAfter.Volumes["USD"].Balance).To(Equal("0"))
		})

		It("Should correctly track volumes after revert", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "volume-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Revert the transaction
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())

			// Verify volumes: input=100, output=100, balance=0
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should mark original transaction as reverted", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Verify transaction is not reverted initially
			tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(tx.Transaction.Reverted).To(BeFalse())

			// Revert the transaction
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())

			// Verify transaction is now marked as reverted
			txAfter, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(txAfter.Transaction.Reverted).To(BeTrue())
		})

		It("Should create a new reverting transaction", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			originalTxID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction
			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, originalTxID, false, false, nil)},
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should fail to revert when account has insufficient funds without force flag", func() {
			// Create a transaction: world -> account-1 (100 USD)
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Spend the funds: account-1 -> account-2 (100 USD)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("account-1", "account-2", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Try to revert the original transaction (account-1 has 0 balance)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("insufficient"))
		})

		It("Should succeed to revert when account has insufficient funds with force flag", func() {
			// Create a transaction: world -> account-1 (100 USD)
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Spend the funds: account-1 -> account-2 (100 USD)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("account-1", "account-2", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Revert with force flag (should succeed even with negative balance)
			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction(ledgerName, transactionID, true, false, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))

			// Verify account-1 has negative balance
			account1, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1.Volumes["USD"].Balance).To(Equal("-100"))
		})
	})

	Context("When reverting on invalid ledger", func() {
		It("Should fail to revert on non-existent ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{revertTransactionAction("non-existent-ledger", 1, false, false, nil)},
			})
			Expect(err).To(HaveOccurred())
		})
	})
})
