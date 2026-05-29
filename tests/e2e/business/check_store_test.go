//go:build e2e

package business

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// collectCheckStoreEvents runs the CheckStore RPC and returns all errors and progress events.
func collectCheckStoreEvents(ctx context.Context, client servicepb.BucketServiceClient) ([]*servicepb.CheckStoreError, []*servicepb.CheckStoreProgress, error) {
	result, err := actions.CollectCheckStoreEvents(ctx, client)
	if err != nil {
		return nil, nil, err
	}

	return result.Errors, result.Progress, nil
}

// expectStoreValid is a reusable assertion that runs CheckStore and expects no errors.
func expectStoreValid(ctx context.Context, client servicepb.BucketServiceClient) {
	errors, progress, err := collectCheckStoreEvents(ctx, client)
	Expect(err).To(Succeed(), "CheckStore RPC should not fail")
	for _, e := range errors {
		GinkgoWriter.Printf("  Check error: [%s] %s (log=%d, ledger=%s, account=%s, asset=%s, tx=%d)\n",
			e.ErrorType, e.Message, e.LogSequence, e.Ledger, e.Account, e.Asset, e.TransactionId)
	}
	Expect(errors).To(BeEmpty(), "store should have no integrity errors")
	Expect(progress).NotTo(BeEmpty(), "should emit at least one progress event")
}

var _ = Describe("CheckStore", Ordered, func() {

	Context("On an empty store", func() {
		It("Should pass with no errors and emit progress", func() {
			errors, progress, err := collectCheckStoreEvents(sharedCtx, sharedClient)
			Expect(err).To(Succeed())
			Expect(errors).To(BeEmpty())
			Expect(progress).NotTo(BeEmpty())
			last := progress[len(progress)-1]
			Expect(last.LogsChecked).To(Equal(last.TotalLogs))
		})
	})

	Context("After creating ledgers", Ordered, func() {
		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("check-ledger-1", nil),
					actions.CreateLedgerAction("check-ledger-2", map[string]string{
						"env":  "test",
						"tier": "premium",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After simple transactions", Ordered, func() {
		var ledgerName = "check-tx-simple"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund accounts
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(100000), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(50000), "EUR"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "treasury", big.NewInt(500000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Transfers
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "user:alice", big.NewInt(1000), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "user:bob", big.NewInt(2500), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "user:alice", big.NewInt(500), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After multi-posting transactions", Ordered, func() {
		var ledgerName = "check-tx-multi"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "treasury", big.NewInt(1000000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Multi-posting transaction
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("treasury", "user:1", big.NewInt(1000), "USD"),
						actions.NewPosting("treasury", "user:2", big.NewInt(2000), "USD"),
						actions.NewPosting("treasury", "user:3", big.NewInt(3000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After account metadata operations", Ordered, func() {
		var ledgerName = "check-meta-account"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create accounts
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user:alice", big.NewInt(1000), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user:bob", big.NewInt(2000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Set metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "user:alice", map[string]string{
						"status": "active", "tier": "gold", "verified": "true",
					}),
					actions.SaveAccountMetadataAction(ledgerName, "user:bob", map[string]string{
						"status": "pending",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Update metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "user:bob", map[string]string{
						"status": "active",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Delete metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DeleteAccountMetadataAction(ledgerName, "user:alice", "tier"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After transaction metadata operations", Ordered, func() {
		var ledgerName = "check-meta-tx"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(10000), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(20000), "EUR"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Extract transaction IDs
			tx1ID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id
			tx2ID := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Save metadata on transactions
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveTransactionMetadataAction(ledgerName, tx1ID, map[string]string{
						"category": "funding", "note": "initial funding",
					}),
					actions.SaveTransactionMetadataAction(ledgerName, tx2ID, map[string]string{
						"category": "fx",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Update transaction metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveTransactionMetadataAction(ledgerName, tx1ID, map[string]string{
						"status": "approved",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Delete transaction metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DeleteTransactionMetadataAction(ledgerName, tx1ID, "note"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After transaction reversions", Ordered, func() {
		var ledgerName = "check-revert"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions to revert
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user:alice", big.NewInt(1000), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user:bob", big.NewInt(2000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			tx1ID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id
			tx2ID := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert first transaction
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RevertTransactionAction(ledgerName, tx1ID, false, false, nil),
				},
			})
			Expect(err).To(Succeed())

			// Revert second transaction with metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RevertTransactionAction(ledgerName, tx2ID, false, false, map[string]string{
						"reason": "test revert",
					}),
				},
			})
			Expect(err).To(Succeed())

			// More transactions after revert
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user:alice", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After force revert with insufficient funds", Ordered, func() {
		var ledgerName = "check-force-revert"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund account, then spend, then force-revert the original
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "spender", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			fundTxID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Spend it all
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("spender", "receiver", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Force revert (spender now has 0 but revert sends back 100 from spender)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RevertTransactionAction(ledgerName, fundTxID, true, false, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After transactions with inline account metadata", Ordered, func() {
		var ledgerName = "check-inline-meta"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Transaction with inline account metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "customer:1", big.NewInt(5000), "USD"),
					}, map[string]string{"type": "deposit"}),
				},
			})
			Expect(err).To(Succeed())

			// Transaction with account metadata on multiple accounts
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "customer:2", big.NewInt(3000), "EUR"),
					}, map[string]string{"type": "fx-deposit"}, map[string]*commonpb.MetadataMap{
						"customer:2": commonpb.MetadataMapFromGoMap(map[string]string{
							"joined":  "2026-01-15",
							"country": "FR",
						}),
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After ledger deletion", Ordered, func() {
		BeforeAll(func() {
			// Create a ledger, operate on it, then delete it
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction("check-to-delete", nil)},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction("check-to-delete", []*commonpb.Posting{
						actions.NewPosting("world", "temp", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.DeleteLedgerAction("check-to-delete")},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After Numscript transactions", Ordered, func() {
		var ledgerName = "check-numscript"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund with force
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(100000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Numscript-based transactions
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateScriptTransactionAction(ledgerName, `
						send [USD 500] (
							source = @bank
							destination = @user:charlie
						)
					`, nil, map[string]string{"via": "numscript"}),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceScriptTransactionAction(ledgerName, `
						send [EUR 1000] (
							source = @world
							destination = @user:charlie
						)
					`, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After a high volume of mixed operations", Ordered, func() {
		var ledgerName = "check-high-volume"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund multiple accounts with multiple assets
			assets := []string{"USD", "EUR", "GBP", "BTC"}
			for _, asset := range assets {
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "treasury", big.NewInt(1000000), asset),
						}, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Distribute to many users
			for i := 1; i <= 10; i++ {
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("treasury", fmt.Sprintf("user:%d", i), big.NewInt(int64(1000*i)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Transfers between users
			for i := 1; i < 5; i++ {
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting(fmt.Sprintf("user:%d", i), fmt.Sprintf("user:%d", i+5), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Account metadata on all users
			for i := 1; i <= 10; i++ {
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.SaveAccountMetadataAction(ledgerName, fmt.Sprintf("user:%d", i), map[string]string{
							"status":     "active",
							"created_by": "high-volume-test",
						}),
					},
				})
				Expect(err).To(Succeed())
			}

			// Delete some metadata
			for i := 1; i <= 3; i++ {
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.DeleteAccountMetadataAction(ledgerName, fmt.Sprintf("user:%d", i), "created_by"),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})

		It("Should report correct progress", func() {
			_, progress, err := collectCheckStoreEvents(sharedCtx, sharedClient)
			Expect(err).To(Succeed())
			Expect(progress).NotTo(BeEmpty())
			last := progress[len(progress)-1]
			Expect(last.LogsChecked).To(Equal(last.TotalLogs))
			Expect(last.TotalLogs).To(BeNumerically(">", 0))
		})
	})

	Context("After multiple ledgers with cross-operations", Ordered, func() {
		BeforeAll(func() {
			// Create multiple ledgers
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("check-multi-a", nil),
					actions.CreateLedgerAction("check-multi-b", nil),
					actions.CreateLedgerAction("check-multi-c", map[string]string{"purpose": "archive"}),
				},
			})
			Expect(err).To(Succeed())

			// Operate on each ledger
			for _, ledger := range []string{"check-multi-a", "check-multi-b", "check-multi-c"} {
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
							actions.NewPosting("world", "bank", big.NewInt(50000), "USD"),
						}, nil),
					},
				})
				Expect(err).To(Succeed())

				_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledger, []*commonpb.Posting{
							actions.NewPosting("bank", "user", big.NewInt(1000), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())

				_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.SaveAccountMetadataAction(ledger, "user", map[string]string{
							"ledger": ledger,
						}),
					},
				})
				Expect(err).To(Succeed())
			}

			// Delete one ledger
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.DeleteLedgerAction("check-multi-c")},
			})
			Expect(err).To(Succeed())

			// Continue operating on remaining ledgers
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction("check-multi-a", []*commonpb.Posting{
						actions.NewPosting("bank", "user", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After revert then metadata on same transaction", Ordered, func() {
		var ledgerName = "check-revert-then-meta"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user", big.NewInt(1000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Add metadata before revert
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveTransactionMetadataAction(ledgerName, txID, map[string]string{
						"pre-revert": "note",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Revert
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RevertTransactionAction(ledgerName, txID, false, false, nil),
				},
			})
			Expect(err).To(Succeed())

			// Add metadata after revert
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveTransactionMetadataAction(ledgerName, txID, map[string]string{
						"post-revert": "reason",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After bulk operations in a single Apply call", Ordered, func() {
		var ledgerName = "check-bulk"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Bulk: multiple transactions + metadata operations in single request
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "user:1", big.NewInt(100), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "user:2", big.NewInt(200), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "user:3", big.NewInt(300), "USD"),
					}, nil, nil),
					actions.SaveAccountMetadataAction(ledgerName, "bank", map[string]string{
						"type": "main-bank",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("After many reversions in sequence", Ordered, func() {
		var ledgerName = "check-many-reverts"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create and immediately revert multiple transactions
			for i := 1; i <= 5; i++ {
				resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("temp:%d", i), big.NewInt(int64(i*100)), "USD"),
						}, nil),
					},
				})
				Expect(err).To(Succeed())

				txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

				_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.RevertTransactionAction(ledgerName, txID, false, false, map[string]string{
							"batch": fmt.Sprintf("%d", i),
						}),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should pass integrity check", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})
	})

	Context("Comprehensive mixed workload", Ordered, func() {
		// This test exercises every type of operation through the full gRPC stack
		// and verifies the store remains consistent.
		BeforeAll(func() {
			// 1. Create ledgers with various configurations
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("check-comprehensive-trading", nil),
					actions.CreateLedgerAction("check-comprehensive-payments", map[string]string{
						"region": "eu", "currency": "EUR",
					}),
				},
			})
			Expect(err).To(Succeed())

			trading := "check-comprehensive-trading"
			payments := "check-comprehensive-payments"

			// 2. Fund multiple accounts in multiple assets across ledgers
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(500000), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(400000), "EUR"),
					}, nil),
					actions.CreateForceTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(300000), "GBP"),
					}, nil),
					actions.CreateForceTransactionAction(payments, []*commonpb.Posting{
						actions.NewPosting("world", "escrow", big.NewInt(100000), "EUR"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// 3. Multi-posting distribution
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("bank", "trader:alpha", big.NewInt(10000), "USD"),
						actions.NewPosting("bank", "trader:beta", big.NewInt(20000), "USD"),
						actions.NewPosting("bank", "trader:gamma", big.NewInt(5000), "EUR"),
					}, map[string]string{"batch": "distribution-1"}, nil),
				},
			})
			Expect(err).To(Succeed())

			// 4. Inter-account transfers
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("trader:alpha", "trader:beta", big.NewInt(1000), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("trader:beta", "trader:gamma", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// 5. Account metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(trading, "trader:alpha", map[string]string{
						"strategy": "aggressive", "level": "pro",
					}),
					actions.SaveAccountMetadataAction(trading, "trader:beta", map[string]string{
						"strategy": "conservative",
					}),
					actions.SaveAccountMetadataAction(payments, "escrow", map[string]string{
						"type": "escrow", "locked": "true",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 6. Transaction metadata
			// Get a transaction ID for metadata
			txResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(payments, []*commonpb.Posting{
						actions.NewPosting("escrow", "merchant:1", big.NewInt(5000), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			paymentTxID := txResp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveTransactionMetadataAction(payments, paymentTxID, map[string]string{
						"invoice": "INV-001", "approved_by": "admin",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 7. Revert a transaction
			revertResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("world", "trader:delta", big.NewInt(999), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			revertTargetID := revertResp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RevertTransactionAction(trading, revertTargetID, false, false, map[string]string{
						"reason": "error correction",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 8. Delete metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DeleteAccountMetadataAction(trading, "trader:alpha", "level"),
					actions.DeleteTransactionMetadataAction(payments, paymentTxID, "approved_by"),
				},
			})
			Expect(err).To(Succeed())

			// 9. More operations after all the above
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(trading, []*commonpb.Posting{
						actions.NewPosting("bank", "trader:alpha", big.NewInt(777), "GBP"),
					}, nil, nil),
					actions.SaveAccountMetadataAction(trading, "trader:alpha", map[string]string{
						"level": "elite",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 10. Numscript transaction
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateScriptTransactionAction(trading, `
						send [USD 250] (
							source = @bank
							destination = @trader:gamma
						)
					`, nil, map[string]string{"via": "numscript"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check with no errors", func() {
			expectStoreValid(sharedCtx, sharedClient)
		})

		It("Should report progress correctly", func() {
			_, progress, err := collectCheckStoreEvents(sharedCtx, sharedClient)
			Expect(err).To(Succeed())

			Expect(progress).NotTo(BeEmpty())
			last := progress[len(progress)-1]
			Expect(last.LogsChecked).To(Equal(last.TotalLogs))
			// We created many logs across all the operations
			Expect(last.TotalLogs).To(BeNumerically(">=", 15))
		})
	})
})
