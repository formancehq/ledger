//go:build e2e

package e2e

import (
	"context"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// timestampToStdTime converts a commonpb.Timestamp to standard time.Time
func timestampToStdTime(ts *commonpb.Timestamp) time.Time {
	return time.UnixMicro(int64(ts.GetData()))
}

// listAllTransactions collects all transactions from the streaming RPC into a slice
func listAllTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, afterTxID uint64) ([]*commonpb.Transaction, error) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:    ledgerName,
		PageSize:  pageSize,
		AfterTxId: afterTxID,
	})
	if err != nil {
		return nil, err
	}

	var transactions []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

var _ = Describe("Transactions", Ordered, func() {
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

	Context("When creating transactions", Ordered, func() {
		var ledgerName = "tx-create-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should create a simple transaction", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "ts-default", big.NewInt(100), "USD"),
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
			req := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "ts-custom", big.NewInt(100), "USD"),
			}, nil, nil)
			withTimestamp(req, customTime)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
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

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-metadata", big.NewInt(100), "USD"),
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
			Expect(createdTx.Transaction.Metadata.ToMap()["description"]).To(Equal("Test transaction"))
			Expect(createdTx.Transaction.Metadata.ToMap()["category"]).To(Equal("test"))
		})

		It("Should create a transaction with account metadata", func() {
			accountMetadata := map[string]*commonpb.MetadataSet{
				"account-with-meta": commonpb.MetadataSetFromMap(map[string]string{
					"account_type": "asset",
					"label":        "Account with Metadata",
				}),
			}

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-with-meta", big.NewInt(100), "USD"),
					}, nil, accountMetadata),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account exists and has correct balance
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
				resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting(tx.source, tx.destination, tx.amount, tx.asset),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed(), "Failed to create transaction %d", i+1)
				Expect(resp).NotTo(BeNil())
				Expect(resp.Logs).To(HaveLen(1))
			}

			// Verify final balances
			account1, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "seq-account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1.Volumes["USD"].Balance).To(Equal("50")) // 100 - 50

			account2, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "seq-account-2",
			})
			Expect(err).To(Succeed())
			Expect(account2.Volumes["USD"].Balance).To(Equal("250")) // 200 + 50
		})

		It("Should create a transaction with multiple postings", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-a", big.NewInt(100), "USD"),
						newPosting("world", "account-b", big.NewInt(200), "USD"),
						newPosting("world", "account-c", big.NewInt(300), "USD"),
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

			accountC, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-c",
			})
			Expect(err).To(Succeed())
			Expect(accountC.Volumes["USD"].Balance).To(Equal("300"))
		})

		It("Should create a transaction with multiple assets", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "multi-asset-account", big.NewInt(100), "USD"),
						newPosting("world", "multi-asset-account", big.NewInt(50), "EUR"),
						newPosting("world", "multi-asset-account", big.NewInt(1000), "JPY"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account has balances in all assets
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-account-1", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-account-2", big.NewInt(200), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-account-3", big.NewInt(300), "USD"),
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "implicit-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// The account should now exist
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "large-amount-account", largeAmount, "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the amount is stored correctly
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should fail when source has insufficient funds", func() {
			// First, fund the account
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "limited-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Try to send more than available
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("limited-account", "destination", big.NewInt(150), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInsufficientFunds))
			Expect(info.Domain).To(Equal("ledger"))
			Expect(info.Metadata["account"]).To(Equal("limited-account"))
			Expect(info.Metadata["asset"]).To(Equal("USD"))
		})

		It("Should fail when ledger does not exist", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("non-existent-ledger", []*commonpb.Posting{
						newPosting("world", "account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonLedgerNotFound))
			Expect(info.Domain).To(Equal("ledger"))
		})

		It("Should allow world account to have negative balance", func() {
			// World can send unlimited funds
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "recipient", big.NewInt(1000000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Recipient should have the exact amount
			recipient, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "recipient",
			})
			Expect(err).To(Succeed())
			Expect(recipient.Volumes["USD"].Balance).To(Equal("1000000"))

			// World's balance should be negative
			world, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should get a transaction by ID", func() {
			// Create a transaction
			createResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "read-account", big.NewInt(100), "USD"),
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
			getResp, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
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
			Expect(getResp.Transaction.Metadata.ToMap()["description"]).To(Equal("Test transaction"))
		})

		It("Should return error for non-existent transaction", func() {
			_, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: 99999,
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When verifying account balances after transactions", Ordered, func() {
		var ledgerName = "tx-balance-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should correctly track input and output volumes", func() {
			// Fund an account
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "volume-account", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Send some out
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("volume-account", "other", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify volumes
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "cycle-a", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("cycle-a", "cycle-b", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("cycle-b", "cycle-c", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("cycle-c", "cycle-a", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// cycle-a should have input=200, output=100, balance=100
			accountA, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 5 transactions
			createdTxIDs = nil
			for i := 0; i < 5; i++ {
				resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "list-account", big.NewInt(int64(100*(i+1))), "USD"),
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
			transactions, err := listAllTransactions(ctx, client, ledgerName, 0, 0)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(5))
		})

		It("Should return transactions in reverse chronological order (newest first)", func() {
			transactions, err := listAllTransactions(ctx, client, ledgerName, 0, 0)
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
			transactions, err := listAllTransactions(ctx, client, ledgerName, 2, 0)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(2))

			// Should be the 2 most recent transactions
			Expect(transactions[0].Id).To(Equal(createdTxIDs[4]))
			Expect(transactions[1].Id).To(Equal(createdTxIDs[3]))
		})

		It("Should paginate with afterTxId", func() {
			// First page: get 2 transactions
			firstPage, err := listAllTransactions(ctx, client, ledgerName, 2, 0)
			Expect(err).To(Succeed())
			Expect(firstPage).To(HaveLen(2))

			// Second page: get 2 more transactions after the last one from first page
			secondPage, err := listAllTransactions(ctx, client, ledgerName, 2, firstPage[1].Id)
			Expect(err).To(Succeed())
			Expect(secondPage).To(HaveLen(2))

			// Verify no overlap between pages
			for _, tx1 := range firstPage {
				for _, tx2 := range secondPage {
					Expect(tx1.Id).NotTo(Equal(tx2.Id))
				}
			}

			// Third page: get remaining transaction
			thirdPage, err := listAllTransactions(ctx, client, ledgerName, 2, secondPage[1].Id)
			Expect(err).To(Succeed())
			Expect(thirdPage).To(HaveLen(1))

			// Fourth page: should be empty
			fourthPage, err := listAllTransactions(ctx, client, ledgerName, 2, thirdPage[0].Id)
			Expect(err).To(Succeed())
			Expect(fourthPage).To(BeEmpty())
		})

		It("Should return empty list for empty ledger", func() {
			// Create a new empty ledger
			emptyLedgerName := "tx-list-empty-ledger"
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(emptyLedgerName, nil)},
			})
			Expect(err).To(Succeed())

			transactions, err := listAllTransactions(ctx, client, emptyLedgerName, 0, 0)
			Expect(err).To(Succeed())
			Expect(transactions).To(BeEmpty())
		})

		It("Should return transaction details including postings and metadata", func() {
			transactions, err := listAllTransactions(ctx, client, ledgerName, 1, 0)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(1))

			tx := transactions[0]
			Expect(tx.Id).To(Equal(createdTxIDs[4]))
			Expect(tx.Postings).To(HaveLen(1))
			Expect(tx.Postings[0].Source).To(Equal("world"))
			Expect(tx.Postings[0].Destination).To(Equal("list-account"))
			Expect(tx.Postings[0].Asset).To(Equal("USD"))
			Expect(tx.Metadata).NotTo(BeNil())
			Expect(tx.Metadata.ToMap()["index"]).To(Equal("E"))
		})

		It("Should handle large page sizes correctly", func() {
			// Request more transactions than exist
			transactions, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(5))
		})

		It("Should handle afterTxId beyond existing transactions", func() {
			// Use a very high afterTxId that doesn't exist
			transactions, err := listAllTransactions(ctx, client, ledgerName, 0, 999999)
			Expect(err).To(Succeed())
			// Should return all transactions since they all have IDs < 999999
			Expect(transactions).To(HaveLen(5))
		})

		It("Should correctly list transactions after bulk creation", func() {
			// Create multiple transactions in bulk
			bulkLedgerName := "tx-list-bulk-ledger"
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(bulkLedgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Bulk create 3 transactions
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(bulkLedgerName, []*commonpb.Posting{
						newPosting("world", "bulk-1", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(bulkLedgerName, []*commonpb.Posting{
						newPosting("world", "bulk-2", big.NewInt(200), "USD"),
					}, nil, nil),
					createTransactionAction(bulkLedgerName, []*commonpb.Posting{
						newPosting("world", "bulk-3", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(3))

			// List and verify all 3 transactions are returned
			transactions, err := listAllTransactions(ctx, client, bulkLedgerName, 0, 0)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(3))
		})
	})
})
