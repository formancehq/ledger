//go:build e2e

package business

import (
	"math"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// timestampToStdTime converts a commonpb.Timestamp to standard time.Time
func timestampToStdTime(ts *commonpb.Timestamp) time.Time {
	return time.UnixMicro(int64(ts.GetData()))
}

var _ = Describe("Transactions", Ordered, func() {

	Context("When creating transactions", Ordered, func() {
		var ledgerName = "tx-create-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should create a simple transaction", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify transaction details in response
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Id).NotTo(BeZero())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
		})

		It("Should use the command date as timestamp when no timestamp is provided", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ts-default", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			applyLog := resp.Logs[0].Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()

			// Timestamp should be set (not nil) and equal to the log date
			Expect(createdTx.Transaction.Timestamp).NotTo(BeNil())
			Expect(createdTx.Transaction.Timestamp.GetData()).To(Equal(applyLog.Log.Date.GetData()))
		})

		It("Should use the user-provided timestamp when specified", func() {
			customTime := time.Date(2020, 6, 15, 12, 0, 0, 0, time.UTC)
			req := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "ts-custom", big.NewInt(100), "USD"),
			}, nil, nil)
			actions.WithTimestamp(req, customTime)

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			applyLog := resp.Logs[0].Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()

			// Timestamp should match the custom value, not the log date
			Expect(createdTx.Transaction.Timestamp).NotTo(BeNil())
			Expect(timestampToStdTime(createdTx.Transaction.Timestamp)).To(BeTemporally("~", customTime, time.Second))
			Expect(createdTx.Transaction.Timestamp.GetData()).NotTo(Equal(applyLog.Log.Date.GetData()))
		})

		It("Should create a transaction with metadata", func() {
			metadata := map[string]string{
				"description": "Test transaction",
				"category":    "test",
			}

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-metadata", big.NewInt(100), "USD"),
					}, metadata, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify metadata in transaction
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Metadata).NotTo(BeNil())
			txMeta := commonpb.MetadataToGoMap(createdTx.Transaction.Metadata)
			Expect(txMeta["description"]).To(Equal("Test transaction"))
			Expect(txMeta["category"]).To(Equal("test"))
		})

		It("Should create a transaction with account metadata", func() {
			accountMetadata := map[string]*commonpb.MetadataMap{
				"account-with-meta": commonpb.MetadataMapFromGoMap(map[string]string{
					"account_type": "asset",
					"label":        "Account with Metadata",
				}),
			}

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-with-meta", big.NewInt(100), "USD"),
					}, nil, accountMetadata),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account exists and has correct balance
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-with-meta",
			})
			Expect(err).To(Succeed())
			Expect(account.Address).To(Equal("account-with-meta"))
			Expect(account.Volumes["USD"].Balance).To(Equal("100"))
		})

		It("Should create multiple transactions sequentially", func() {
			transactions := []struct {
				source      string
				destination string
				amount      *big.Int
				asset       string
			}{
				{"world", "seq-account-1", big.NewInt(100), "USD"},
				{"world", "seq-account-2", big.NewInt(200), "USD"},
				{"seq-account-1", "seq-account-2", big.NewInt(50), "USD"},
			}

			for i, tx := range transactions {
				resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting(tx.source, tx.destination, tx.amount, tx.asset),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed(), "Failed to create transaction %d", i+1)
				Expect(resp).NotTo(BeNil())
				Expect(resp.Logs).To(HaveLen(1))
			}

			// Verify final balances
			account1, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "seq-account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1.Volumes["USD"].Balance).To(Equal("50")) // 100 - 50

			account2, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "seq-account-2",
			})
			Expect(err).To(Succeed())
			Expect(account2.Volumes["USD"].Balance).To(Equal("250")) // 200 + 50
		})

		It("Should create a transaction with multiple postings", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-a", big.NewInt(100), "USD"),
						actions.NewPosting("world", "account-b", big.NewInt(200), "USD"),
						actions.NewPosting("world", "account-c", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify transaction has multiple postings
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(3))

			// Verify all accounts have correct balances
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

			accountC, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-c",
			})
			Expect(err).To(Succeed())
			Expect(accountC.Volumes["USD"].Balance).To(Equal("300"))
		})

		It("Should create a transaction with multiple assets", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "multi-asset-account", big.NewInt(100), "USD"),
						actions.NewPosting("world", "multi-asset-account", big.NewInt(50), "EUR"),
						actions.NewPosting("world", "multi-asset-account", big.NewInt(1000), "JPY"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account has balances in all assets
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "multi-asset-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes).To(HaveLen(3))
			Expect(account.Volumes["USD"].Balance).To(Equal("100"))
			Expect(account.Volumes["EUR"].Balance).To(Equal("50"))
			Expect(account.Volumes["JPY"].Balance).To(Equal("1000"))
		})

		It("Should create multiple transactions in bulk", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bulk-account-1", big.NewInt(100), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bulk-account-2", big.NewInt(200), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bulk-account-3", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(3))

			// Verify each transaction has unique ID
			ids := make(map[uint64]bool)
			for _, log := range resp.Logs {
				applyLog := log.Payload.GetApply()
				txID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
				Expect(ids[txID]).To(BeFalse(), "Transaction IDs should be unique")
				ids[txID] = true
			}
		})

		It("Should create accounts implicitly via transaction", func() {
			// Create a transaction to a new account
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "implicit-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// The account should now exist
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "implicit-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Address).To(Equal("implicit-account"))
			Expect(account.Volumes["USD"].Balance).To(Equal("100"))
		})

		It("Should handle large amounts correctly", func() {
			// Use a very large number (greater than int64)
			largeAmount := new(big.Int)
			largeAmount.SetString("99999999999999999999999999999", 10)

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "large-amount-account", largeAmount, "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the amount is stored correctly
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "large-amount-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Balance).To(Equal("99999999999999999999999999999"))
		})
	})

	Context("When creating transactions with validation errors", Ordered, func() {
		var ledgerName = "tx-validation-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should fail when source has insufficient funds", func() {
			// First, fund the account
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "limited-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Try to send more than available
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("limited-account", "destination", big.NewInt(150), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInsufficientFunds))
			Expect(info.Domain).To(Equal("ledger"))
			Expect(info.Metadata["account"]).To(Equal("limited-account"))
			Expect(info.Metadata["asset"]).To(Equal("USD"))
		})

		It("Should fail when ledger does not exist", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction("non-existent-ledger", []*commonpb.Posting{
						actions.NewPosting("world", "account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonLedgerNotFound))
			Expect(info.Domain).To(Equal("ledger"))
		})

		It("Should allow world account to have negative balance", func() {
			// World can send unlimited funds
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "recipient", big.NewInt(1000000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Recipient should have the exact amount
			recipient, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "recipient",
			})
			Expect(err).To(Succeed())
			Expect(recipient.Volumes["USD"].Balance).To(Equal("1000000"))

			// World's balance should be negative
			world, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "world",
			})
			Expect(err).To(Succeed())
			Expect(world.Volumes["USD"].Balance).To(HavePrefix("-"))
		})
	})

	Context("When reading transactions", Ordered, func() {
		var ledgerName = "tx-read-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should get a transaction by ID", func() {
			// Create a transaction
			createResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "read-account", big.NewInt(100), "USD"),
					}, map[string]string{"description": "Test transaction"}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp.Logs).To(HaveLen(1))

			// Extract transaction ID
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Get the transaction
			getResp, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(getResp).NotTo(BeNil())
			Expect(getResp.Transaction.Id).To(Equal(transactionID))
			Expect(getResp.Transaction.Postings).To(HaveLen(1))
			Expect(getResp.Transaction.Postings[0].Source).To(Equal("world"))
			Expect(getResp.Transaction.Postings[0].Destination).To(Equal("read-account"))
			Expect(getResp.Transaction.Postings[0].Asset).To(Equal("USD"))
			Expect(commonpb.MetadataToGoMap(getResp.Transaction.Metadata)["description"]).To(Equal("Test transaction"))
		})

		It("Should return error for non-existent transaction", func() {
			_, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: 99999,
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When verifying account balances after transactions", Ordered, func() {
		var ledgerName = "tx-balance-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should correctly track input and output volumes", func() {
			// Fund an account
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "volume-account", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Send some out
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("volume-account", "other", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify volumes
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "volume-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Input).To(Equal("1000"))
			Expect(account.Volumes["USD"].Output).To(Equal("300"))
			Expect(account.Volumes["USD"].Balance).To(Equal("700"))
		})

		It("Should handle circular transactions correctly", func() {
			// A -> B -> C -> A cycle
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "cycle-a", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("cycle-a", "cycle-b", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("cycle-b", "cycle-c", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("cycle-c", "cycle-a", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// cycle-a should have input=200, output=100, balance=100
			accountA, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "cycle-a",
			})
			Expect(err).To(Succeed())
			Expect(accountA.Volumes["USD"].Input).To(Equal("200"))  // from world + cycle-c
			Expect(accountA.Volumes["USD"].Output).To(Equal("100")) // to cycle-b
			Expect(accountA.Volumes["USD"].Balance).To(Equal("100"))
		})
	})

	Context("When listing transactions", Ordered, func() {
		var ledgerName = "tx-list-ledger"
		var createdTxIDs []uint64

		BeforeAll(func() {
			// Create ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 5 transactions
			createdTxIDs = nil
			for i := 0; i < 5; i++ {
				resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "list-account", big.NewInt(int64(100*(i+1))), "USD"),
						}, map[string]string{"index": string(rune('A' + i))}, nil),
					},
				})
				Expect(err).To(Succeed())
				Expect(resp.Logs).To(HaveLen(1))

				log := resp.Logs[0]
				applyLog := log.Payload.GetApply()
				txID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
				createdTxIDs = append(createdTxIDs, txID)
			}
		})

		It("Should list all transactions", func() {
			// The Pebble read index is populated asynchronously by the index builder,
			// so we need to wait for it to catch up after writing data.
			Eventually(func(g Gomega) {
				transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, nil)
				g.Expect(err).To(Succeed())
				g.Expect(transactions).To(HaveLen(5))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should return transactions in reverse chronological order (newest first)", func() {
			transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, nil)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(5))

			// Verify descending order by ID
			for i := 0; i < len(transactions)-1; i++ {
				Expect(transactions[i].Id).To(BeNumerically(">", transactions[i+1].Id))
			}

			// Verify the first transaction is the newest one created
			Expect(transactions[0].Id).To(Equal(createdTxIDs[4]))
			// Verify the last transaction is the oldest one created
			Expect(transactions[4].Id).To(Equal(createdTxIDs[0]))
		})

		It("Should respect page size limit", func() {
			transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 2, 0, nil)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(2))

			// Should be the 2 most recent transactions
			Expect(transactions[0].Id).To(Equal(createdTxIDs[4]))
			Expect(transactions[1].Id).To(Equal(createdTxIDs[3]))
		})

		It("Should paginate with afterTxId", func() {
			// First page: get 2 transactions
			firstPage, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 2, 0, nil)
			Expect(err).To(Succeed())
			Expect(firstPage).To(HaveLen(2))

			// Second page: get 2 more transactions after the last one from first page
			secondPage, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 2, firstPage[1].Id, nil)
			Expect(err).To(Succeed())
			Expect(secondPage).To(HaveLen(2))

			// Verify no overlap between pages
			for _, tx1 := range firstPage {
				for _, tx2 := range secondPage {
					Expect(tx1.Id).NotTo(Equal(tx2.Id))
				}
			}

			// Third page: get remaining transaction
			thirdPage, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 2, secondPage[1].Id, nil)
			Expect(err).To(Succeed())
			Expect(thirdPage).To(HaveLen(1))

			// Fourth page: should be empty
			fourthPage, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 2, thirdPage[0].Id, nil)
			Expect(err).To(Succeed())
			Expect(fourthPage).To(BeEmpty())
		})

		It("Should return empty list for empty ledger", func() {
			// Create a new empty ledger
			emptyLedgerName := "tx-list-empty-ledger"
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(emptyLedgerName, nil)},
			})
			Expect(err).To(Succeed())

			transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, emptyLedgerName, 0, 0, nil)
			Expect(err).To(Succeed())
			Expect(transactions).To(BeEmpty())
		})

		It("Should return transaction details including postings and metadata", func() {
			transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 1, 0, nil)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(1))

			tx := transactions[0]
			Expect(tx.Id).To(Equal(createdTxIDs[4]))
			Expect(tx.Postings).To(HaveLen(1))
			Expect(tx.Postings[0].Source).To(Equal("world"))
			Expect(tx.Postings[0].Destination).To(Equal("list-account"))
			Expect(tx.Postings[0].Asset).To(Equal("USD"))
			Expect(tx.Metadata).NotTo(BeNil())
			Expect(commonpb.MetadataToGoMap(tx.Metadata)["index"]).To(Equal("E"))
		})

		It("Should handle large page sizes correctly", func() {
			// Request more transactions than exist
			transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 100, 0, nil)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(5))
		})

		It("Should handle afterTxId beyond existing transactions", func() {
			// Use a very high afterTxId that doesn't exist
			transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 999999, nil)
			Expect(err).To(Succeed())
			// Should return all transactions since they all have IDs < 999999
			Expect(transactions).To(HaveLen(5))
		})

		It("Should correctly list transactions after bulk creation", func() {
			// Create multiple transactions in bulk
			bulkLedgerName := "tx-list-bulk-ledger"
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(bulkLedgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Bulk create 3 transactions
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(bulkLedgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bulk-1", big.NewInt(100), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(bulkLedgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bulk-2", big.NewInt(200), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(bulkLedgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bulk-3", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(3))

			// The Pebble read index is populated asynchronously by the index builder,
			// so we need to wait for it to catch up after writing data.
			Eventually(func(g Gomega) {
				transactions, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, bulkLedgerName, 0, 0, nil)
				g.Expect(err).To(Succeed())
				g.Expect(transactions).To(HaveLen(3))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When creating transactions with expandVolumes", Ordered, func() {
		var ledgerName = "tx-expand-volumes-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should not include postCommitVolumes when expandVolumes is false", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-no-expand", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.PostCommitVolumes).To(BeNil())
		})

		It("Should include postCommitVolumes for a simple transaction", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-simple", big.NewInt(100), "USD"),
					}, nil, nil)),
				},
			})
			Expect(err).To(Succeed())

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.PostCommitVolumes).NotTo(BeNil())

			pcv := createdTx.PostCommitVolumes.VolumesByAccount
			Expect(pcv).To(HaveKey("world"))
			Expect(pcv).To(HaveKey("ev-simple"))

			// world is shared across tests in this Ordered context, so only check presence
			Expect(pcv["world"].Volumes).To(HaveKey("USD"))

			// ev-simple is fresh — exact values are predictable
			Expect(pcv["ev-simple"].Volumes).To(HaveKey("USD"))
			Expect(pcv["ev-simple"].Volumes["USD"].Input).To(Equal("100"))
			Expect(pcv["ev-simple"].Volumes["USD"].Output).To(Equal("0"))
		})

		It("Should include correct volumes for multiple postings", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-multi-a", big.NewInt(100), "USD"),
						actions.NewPosting("world", "ev-multi-b", big.NewInt(200), "USD"),
					}, nil, nil)),
				},
			})
			Expect(err).To(Succeed())

			pcv := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv).To(HaveKey("world"))
			Expect(pcv).To(HaveKey("ev-multi-a"))
			Expect(pcv).To(HaveKey("ev-multi-b"))

			Expect(pcv["ev-multi-a"].Volumes["USD"].Input).To(Equal("100"))
			Expect(pcv["ev-multi-a"].Volumes["USD"].Output).To(Equal("0"))
			Expect(pcv["ev-multi-b"].Volumes["USD"].Input).To(Equal("200"))
			Expect(pcv["ev-multi-b"].Volumes["USD"].Output).To(Equal("0"))
		})

		It("Should include correct volumes for multiple assets", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-multi-asset", big.NewInt(100), "USD"),
						actions.NewPosting("world", "ev-multi-asset", big.NewInt(50), "EUR"),
					}, nil, nil)),
				},
			})
			Expect(err).To(Succeed())

			pcv := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv).To(HaveKey("ev-multi-asset"))

			vols := pcv["ev-multi-asset"].Volumes
			Expect(vols).To(HaveKey("USD"))
			Expect(vols).To(HaveKey("EUR"))
			Expect(vols["USD"].Input).To(Equal("100"))
			Expect(vols["USD"].Output).To(Equal("0"))
			Expect(vols["EUR"].Input).To(Equal("50"))
			Expect(vols["EUR"].Output).To(Equal("0"))
		})

		It("Should reflect cumulative volumes across sequential transactions", func() {
			resp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-cumul", big.NewInt(500), "USD"),
					}, nil, nil)),
				},
			})
			Expect(err).To(Succeed())

			pcv1 := resp1.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv1["ev-cumul"].Volumes["USD"].Input).To(Equal("500"))
			Expect(pcv1["ev-cumul"].Volumes["USD"].Output).To(Equal("0"))

			resp2, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("ev-cumul", "ev-cumul-dest", big.NewInt(200), "USD"),
					}, nil, nil)),
				},
			})
			Expect(err).To(Succeed())

			pcv2 := resp2.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv2["ev-cumul"].Volumes["USD"].Input).To(Equal("500"))
			Expect(pcv2["ev-cumul"].Volumes["USD"].Output).To(Equal("200"))
			Expect(pcv2["ev-cumul-dest"].Volumes["USD"].Input).To(Equal("200"))
			Expect(pcv2["ev-cumul-dest"].Volumes["USD"].Output).To(Equal("0"))
		})

		It("Should work with force flag and expandVolumes", func() {
			req := actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("ev-force-src", "ev-force-dst", big.NewInt(100), "USD"),
			}, nil)
			actions.WithExpandVolumes(req)

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())

			pcv := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv).To(HaveKey("ev-force-src"))
			Expect(pcv).To(HaveKey("ev-force-dst"))

			Expect(pcv["ev-force-src"].Volumes["USD"].Input).To(Equal("0"))
			Expect(pcv["ev-force-src"].Volumes["USD"].Output).To(Equal("100"))
			Expect(pcv["ev-force-dst"].Volumes["USD"].Input).To(Equal("100"))
			Expect(pcv["ev-force-dst"].Volumes["USD"].Output).To(Equal("0"))
		})

		It("Should include postCommitVolumes with Numscript transaction", func() {
			script := `send [USD/2 100] (
				source = @world
				destination = @user:001
			)`
			req := actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)
			actions.WithExpandVolumes(req)

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.PostCommitVolumes).NotTo(BeNil())

			pcv := createdTx.PostCommitVolumes.VolumesByAccount
			Expect(pcv).To(HaveKey("world"))
			Expect(pcv).To(HaveKey("user:001"))
			Expect(pcv["user:001"].Volumes["USD/2"].Input).To(Equal("100"))
			Expect(pcv["user:001"].Volumes["USD/2"].Output).To(Equal("0"))
		})

		It("Should include postCommitVolumes for each transaction in a bulk request", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-bulk-a", big.NewInt(100), "USD"),
					}, nil, nil)),
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-bulk-b", big.NewInt(200), "USD"),
					}, nil, nil)),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			pcv1 := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv1["ev-bulk-a"].Volumes["USD"].Input).To(Equal("100"))

			pcv2 := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv2["ev-bulk-b"].Volumes["USD"].Input).To(Equal("200"))
		})

		It("Should allow mixing expandVolumes=true and expandVolumes=false in bulk", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-bulk-mix-a", big.NewInt(100), "USD"),
					}, nil, nil)),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "ev-bulk-mix-b", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			ct1 := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(ct1.PostCommitVolumes).NotTo(BeNil())

			ct2 := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(ct2.PostCommitVolumes).To(BeNil())
		})
	})

	Context("When listing transactions with metadata filter", Ordered, func() {
		var ledgerName = "tx-filter-ledger"

		BeforeAll(func() {
			// Create ledger and metadata indexes for the fields we'll filter on
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
					actions.CreateTransactionMetadataIndexAction(ledgerName, "category"),
					actions.CreateTransactionMetadataIndexAction(ledgerName, "priority"),
					actions.CreateTransactionMetadataIndexAction(ledgerName, "tier"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "priority")).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tier")).To(Succeed())

			// Create transactions with various metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, map[string]string{"category": "payment", "priority": "high"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(200), "EUR"),
					}, map[string]string{"category": "refund", "priority": "low"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "merchants:shop1", big.NewInt(300), "USD"),
					}, map[string]string{"category": "payment", "priority": "low"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:charlie", big.NewInt(400), "GBP"),
					}, map[string]string{"category": "transfer"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "merchants:shop2", big.NewInt(500), "USD"),
					}, nil, nil), // No metadata
				},
			})
			Expect(err).To(Succeed())

			// Wait for the index builder to catch up
			Eventually(func(g Gomega) {
				txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, nil)
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(5))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter transactions by exact metadata string value", func() {
			filter := actions.StringMetadataFilter("category", "payment")
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))

			// Verify all returned transactions have category=payment
			for _, tx := range txs {
				Expect(tx.Metadata).NotTo(BeNil())
				Expect(commonpb.MetadataToGoMap(tx.Metadata)["category"]).To(Equal("payment"))
			}
		})

		It("Should filter transactions returning single match", func() {
			filter := actions.StringMetadataFilter("category", "refund")
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(1))
			Expect(commonpb.MetadataToGoMap(txs[0].Metadata)["category"]).To(Equal("refund"))
		})

		It("Should return empty list when no transactions match the filter", func() {
			filter := actions.StringMetadataFilter("category", "nonexistent")
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(BeEmpty())
		})

		It("Should filter transactions by metadata key existence", func() {
			filter := actions.ExistsMetadataFilter("priority")
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			// 3 transactions have "priority" metadata (alice, bob, shop1)
			Expect(txs).To(HaveLen(3))
		})

		It("Should combine filters with AND", func() {
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_And{
					And: &commonpb.AndFilter{
						Filters: []*commonpb.QueryFilter{
							actions.StringMetadataFilter("category", "payment"),
							actions.StringMetadataFilter("priority", "high"),
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(1))
			txMeta := commonpb.MetadataToGoMap(txs[0].Metadata)
			Expect(txMeta["category"]).To(Equal("payment"))
			Expect(txMeta["priority"]).To(Equal("high"))
		})

		It("Should combine filters with OR", func() {
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Or{
					Or: &commonpb.OrFilter{
						Filters: []*commonpb.QueryFilter{
							actions.StringMetadataFilter("category", "refund"),
							actions.StringMetadataFilter("category", "transfer"),
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
		})

		It("Should negate a filter with NOT", func() {
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Not{
					Not: &commonpb.NotFilter{
						Filter: actions.StringMetadataFilter("category", "payment"),
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			// 5 total - 2 with category=payment = 3
			Expect(txs).To(HaveLen(3))
		})

		It("Should respect pagination with filter", func() {
			filter := actions.ExistsMetadataFilter("priority")

			// Get first page (2 items)
			firstPage, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 2, 0, filter)
			Expect(err).To(Succeed())
			Expect(firstPage).To(HaveLen(2))

			// Results should be newest first
			Expect(firstPage[0].Id).To(BeNumerically(">", firstPage[1].Id))

			// Get second page
			secondPage, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 2, firstPage[1].Id, filter)
			Expect(err).To(Succeed())
			Expect(secondPage).To(HaveLen(1))

			// No overlap
			for _, tx1 := range firstPage {
				for _, tx2 := range secondPage {
					Expect(tx1.Id).NotTo(Equal(tx2.Id))
				}
			}
		})

		It("Should return filtered results in newest-first order", func() {
			filter := actions.StringMetadataFilter("category", "payment")
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
			// Newest first
			Expect(txs[0].Id).To(BeNumerically(">", txs[1].Id))
		})

		It("Should isolate transaction metadata from account metadata", func() {
			// Set account metadata on users:alice
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "users:alice", map[string]string{
						"tier": "gold",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Filter by tier=gold on transaction metadata should return nothing,
			// because "tier" is only on the account, not on any transaction.
			Eventually(func(g Gomega) {
				filter := actions.StringMetadataFilter("tier", "gold")
				txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(BeEmpty())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When listing transactions with metadata range filter", Ordered, func() {
		var ledgerName = "tx-range-filter-ledger"

		BeforeAll(func() {
			// Create ledger with int64 schema for transaction "score" and its index
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "score",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
					actions.CreateTransactionMetadataIndexAction(ledgerName, "score"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "score")).To(Succeed())

			// Create transactions with varying "score" metadata
			// tx1: score=10, tx2: score=30, tx3: score=50, tx4: score=70, tx5: no score
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "a1", big.NewInt(100), "USD"),
					}, map[string]string{"score": "10"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "a2", big.NewInt(200), "USD"),
					}, map[string]string{"score": "30"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "a3", big.NewInt(300), "USD"),
					}, map[string]string{"score": "50"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "a4", big.NewInt(400), "USD"),
					}, map[string]string{"score": "70"}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "a5", big.NewInt(500), "USD"),
					}, nil, nil), // No score metadata
				},
			})
			Expect(err).To(Succeed())

			// Wait for the index builder to catch up
			Eventually(func(g Gomega) {
				txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, nil)
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(5))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter transactions with > (greater than)", func() {
			// score > 30 should match tx3(50), tx4(70)
			val := int64(30)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "score"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Min:          &val,
								MinExclusive: true,
							},
						},
					},
				},
			}
			Eventually(func(g Gomega) {
				txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(2))
				for _, tx := range txs {
					g.Expect(tx.Metadata).NotTo(BeNil())
				}
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter transactions with >= (greater than or equal)", func() {
			// score >= 30 should match tx2(30), tx3(50), tx4(70)
			val := int64(30)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "score"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Min: &val,
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(3))
		})

		It("Should filter transactions with < (less than)", func() {
			// score < 50 should match tx1(10), tx2(30)
			val := int64(50)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "score"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Max:          &val,
								MaxExclusive: true,
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
		})

		It("Should filter transactions with <= (less than or equal)", func() {
			// score <= 50 should match tx1(10), tx2(30), tx3(50)
			val := int64(50)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "score"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Max: &val,
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(3))
		})

		It("Should filter transactions with combined range (>= AND <=)", func() {
			// score >= 20 AND score <= 60 should match tx2(30), tx3(50)
			minVal := int64(20)
			maxVal := int64(60)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_And{
					And: &commonpb.AndFilter{
						Filters: []*commonpb.QueryFilter{
							{
								Filter: &commonpb.QueryFilter_Field{
									Field: &commonpb.FieldCondition{
										Field: &commonpb.FieldRef{Metadata: "score"},
										Condition: &commonpb.FieldCondition_IntCond{
											IntCond: &commonpb.IntCondition{
												Min: &minVal,
											},
										},
									},
								},
							},
							{
								Filter: &commonpb.QueryFilter_Field{
									Field: &commonpb.FieldCondition{
										Field: &commonpb.FieldRef{Metadata: "score"},
										Condition: &commonpb.FieldCondition_IntCond{
											IntCond: &commonpb.IntCondition{
												Max: &maxVal,
											},
										},
									},
								},
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
		})

		It("Should return empty list when no transactions match the range", func() {
			// score > 100 should match nobody
			val := int64(100)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "score"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Min:          &val,
								MinExclusive: true,
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(BeEmpty())
		})

		It("Should return empty when filter is `score > MaxInt64` (impossible)", func() {
			// `score > MaxInt64` matches nothing; previously the compiler
			// incremented v++ and wrapped to MinInt64, turning this into
			// `score >= MinInt64` which returned every indexed row.
			val := int64(math.MaxInt64)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "score"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Min:          &val,
								MinExclusive: true,
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(BeEmpty())
		})

		It("Should match all indexed rows when filter is `score <= MaxInt64` (full range)", func() {
			// `score <= MaxInt64` is the entire representable range; the
			// compiler previously incremented v++ and wrapped to MinInt64,
			// flipping the upper bound and returning zero rows.
			val := int64(math.MaxInt64)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "score"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Max: &val,
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(4)) // tx1..tx4 have score; tx5 doesn't
		})
	})

	Context("When listing transactions with source/destination filter", Ordered, func() {
		var ledgerName = "tx-src-dst-filter-ledger"

		BeforeAll(func() {
			// Create ledger with all address indexes (any, source, destination)
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_SOURCE),
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForAddressIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY)).To(Succeed())
			Expect(actions.WaitForAddressIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.AddressRole_ADDRESS_ROLE_SOURCE)).To(Succeed())
			Expect(actions.WaitForAddressIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION)).To(Succeed())

			// Create transactions:
			// tx1: A → B
			// tx2: A → C
			// tx3: B → A
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("A", "B", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("A", "C", big.NewInt(200), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("B", "A", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Wait for the index builder to catch up
			Eventually(func(g Gomega) {
				txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, nil)
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(3))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter by source prefix", func() {
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Address{
					Address: &commonpb.AddressMatch{
						Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "A"},
						Role:  commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			// A is source in tx1 (A→B) and tx2 (A→C)
			Expect(txs).To(HaveLen(2))
			for _, tx := range txs {
				hasSourceA := false
				for _, p := range tx.Postings {
					if p.Source == "A" {
						hasSourceA = true
						break
					}
				}
				Expect(hasSourceA).To(BeTrue())
			}
		})

		It("Should filter by destination exact", func() {
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Address{
					Address: &commonpb.AddressMatch{
						Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "A"},
						Role:  commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			// A is destination only in tx3 (B→A)
			Expect(txs).To(HaveLen(1))
			Expect(txs[0].Postings[0].Destination).To(Equal("A"))
		})

		It("Should filter by source AND destination", func() {
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_And{
					And: &commonpb.AndFilter{
						Filters: []*commonpb.QueryFilter{
							{
								Filter: &commonpb.QueryFilter_Address{
									Address: &commonpb.AddressMatch{
										Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "A"},
										Role:  commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
									},
								},
							},
							{
								Filter: &commonpb.QueryFilter_Address{
									Address: &commonpb.AddressMatch{
										Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "B"},
										Role:  commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
									},
								},
							},
						},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			// Only tx1 (A→B) has source=A AND destination=B
			Expect(txs).To(HaveLen(1))
			Expect(txs[0].Postings[0].Source).To(Equal("A"))
			Expect(txs[0].Postings[0].Destination).To(Equal("B"))
		})

		It("Should still support address filter for backward compatibility", func() {
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Address{
					Address: &commonpb.AddressMatch{
						Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "A"},
					},
				},
			}
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
			Expect(err).To(Succeed())
			// A appears in all 3 transactions (as source or destination)
			Expect(txs).To(HaveLen(3))
		})
	})
})

