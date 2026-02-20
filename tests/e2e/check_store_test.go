//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// collectCheckStoreEvents runs the CheckStore RPC and returns all errors and progress events.
func collectCheckStoreEvents(ctx context.Context, client servicepb.BucketServiceClient) ([]*servicepb.CheckStoreError, []*servicepb.CheckStoreProgress, error) {
	stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
	if err != nil {
		return nil, nil, err
	}

	var (
		errors   []*servicepb.CheckStoreError
		progress []*servicepb.CheckStoreProgress
	)
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		switch t := event.Type.(type) {
		case *servicepb.CheckStoreEvent_Error:
			errors = append(errors, t.Error)
		case *servicepb.CheckStoreEvent_Progress:
			progress = append(progress, t.Progress)
		}
	}

	return errors, progress, nil
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

	Context("On an empty store", func() {
		It("Should pass with no errors and emit progress", func() {
			errors, progress, err := collectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(errors).To(BeEmpty())
			Expect(progress).NotTo(BeEmpty())
			last := progress[len(progress)-1]
			Expect(last.LogsChecked).To(Equal(last.TotalLogs))
		})
	})

	Context("After creating ledgers", Ordered, func() {
		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("check-ledger-1", nil),
					createLedgerAction("check-ledger-2", map[string]string{
						"env":  "test",
						"tier": "premium",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After simple transactions", Ordered, func() {
		var ledgerName = "check-tx-simple"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund accounts
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(100000), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(50000), "EUR"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "treasury", big.NewInt(500000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Transfers
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "user:alice", big.NewInt(1000), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "user:bob", big.NewInt(2500), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "user:alice", big.NewInt(500), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After multi-posting transactions", Ordered, func() {
		var ledgerName = "check-tx-multi"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "treasury", big.NewInt(1000000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Multi-posting transaction
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("treasury", "user:1", big.NewInt(1000), "USD"),
						newPosting("treasury", "user:2", big.NewInt(2000), "USD"),
						newPosting("treasury", "user:3", big.NewInt(3000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After account metadata operations", Ordered, func() {
		var ledgerName = "check-meta-account"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create accounts
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:alice", big.NewInt(1000), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:bob", big.NewInt(2000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Set metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "user:alice", map[string]string{
						"status": "active", "tier": "gold", "verified": "true",
					}),
					saveAccountMetadataAction(ledgerName, "user:bob", map[string]string{
						"status": "pending",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Update metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "user:bob", map[string]string{
						"status": "active",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Delete metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					deleteAccountMetadataAction(ledgerName, "user:alice", "tier"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After transaction metadata operations", Ordered, func() {
		var ledgerName = "check-meta-tx"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(10000), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(20000), "EUR"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Extract transaction IDs
			tx1ID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id
			tx2ID := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Save metadata on transactions
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(ledgerName, tx1ID, map[string]string{
						"category": "funding", "note": "initial funding",
					}),
					saveTransactionMetadataAction(ledgerName, tx2ID, map[string]string{
						"category": "fx",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Update transaction metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(ledgerName, tx1ID, map[string]string{
						"status": "approved",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Delete transaction metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					deleteTransactionMetadataAction(ledgerName, tx1ID, "note"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After transaction reversions", Ordered, func() {
		var ledgerName = "check-revert"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions to revert
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:alice", big.NewInt(1000), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:bob", big.NewInt(2000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			tx1ID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id
			tx2ID := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert first transaction
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					revertTransactionAction(ledgerName, tx1ID, false, false, nil),
				},
			})
			Expect(err).To(Succeed())

			// Revert second transaction with metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					revertTransactionAction(ledgerName, tx2ID, false, false, map[string]string{
						"reason": "test revert",
					}),
				},
			})
			Expect(err).To(Succeed())

			// More transactions after revert
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:alice", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After force revert with insufficient funds", Ordered, func() {
		var ledgerName = "check-force-revert"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund account, then spend, then force-revert the original
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "spender", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			fundTxID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Spend it all
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("spender", "receiver", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Force revert (spender now has 0 but revert sends back 100 from spender)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					revertTransactionAction(ledgerName, fundTxID, true, false, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After transactions with inline account metadata", Ordered, func() {
		var ledgerName = "check-inline-meta"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Transaction with inline account metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "customer:1", big.NewInt(5000), "USD"),
					}, map[string]string{"type": "deposit"}),
				},
			})
			Expect(err).To(Succeed())

			// Transaction with account metadata on multiple accounts
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "customer:2", big.NewInt(3000), "EUR"),
					}, map[string]string{"type": "fx-deposit"}, map[string]*commonpb.MetadataSet{
						"customer:2": commonpb.MetadataSetFromMap(map[string]string{
							"joined":  "2026-01-15",
							"country": "FR",
						}),
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After ledger deletion", Ordered, func() {
		BeforeAll(func() {
			// Create a ledger, operate on it, then delete it
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction("check-to-delete", nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction("check-to-delete", []*commonpb.Posting{
						newPosting("world", "temp", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteLedgerAction("check-to-delete")},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After Numscript transactions", Ordered, func() {
		var ledgerName = "check-numscript"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund with force
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(100000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Numscript-based transactions
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, `
						send [USD 500] (
							source = @bank
							destination = @user:charlie
						)
					`, nil, map[string]string{"via": "numscript"}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceScriptTransactionAction(ledgerName, `
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
			expectStoreValid(ctx, client)
		})
	})

	Context("After a high volume of mixed operations", Ordered, func() {
		var ledgerName = "check-high-volume"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund multiple accounts with multiple assets
			assets := []string{"USD", "EUR", "GBP", "BTC"}
			for _, asset := range assets {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createForceTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "treasury", big.NewInt(1000000), asset),
						}, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Distribute to many users
			for i := 1; i <= 10; i++ {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("treasury", fmt.Sprintf("user:%d", i), big.NewInt(int64(1000*i)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Transfers between users
			for i := 1; i < 5; i++ {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting(fmt.Sprintf("user:%d", i), fmt.Sprintf("user:%d", i+5), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Account metadata on all users
			for i := 1; i <= 10; i++ {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						saveAccountMetadataAction(ledgerName, fmt.Sprintf("user:%d", i), map[string]string{
							"status":     "active",
							"created_by": "high-volume-test",
						}),
					},
				})
				Expect(err).To(Succeed())
			}

			// Delete some metadata
			for i := 1; i <= 3; i++ {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						deleteAccountMetadataAction(ledgerName, fmt.Sprintf("user:%d", i), "created_by"),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})

		It("Should report correct progress", func() {
			_, progress, err := collectCheckStoreEvents(ctx, client)
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("check-multi-a", nil),
					createLedgerAction("check-multi-b", nil),
					createLedgerAction("check-multi-c", map[string]string{"purpose": "archive"}),
				},
			})
			Expect(err).To(Succeed())

			// Operate on each ledger
			for _, ledger := range []string{"check-multi-a", "check-multi-b", "check-multi-c"} {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createForceTransactionAction(ledger, []*commonpb.Posting{
							newPosting("world", "bank", big.NewInt(50000), "USD"),
						}, nil),
					},
				})
				Expect(err).To(Succeed())

				_, err = client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledger, []*commonpb.Posting{
							newPosting("bank", "user", big.NewInt(1000), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())

				_, err = client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						saveAccountMetadataAction(ledger, "user", map[string]string{
							"ledger": ledger,
						}),
					},
				})
				Expect(err).To(Succeed())
			}

			// Delete one ledger
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteLedgerAction("check-multi-c")},
			})
			Expect(err).To(Succeed())

			// Continue operating on remaining ledgers
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("check-multi-a", []*commonpb.Posting{
						newPosting("bank", "user", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After revert then metadata on same transaction", Ordered, func() {
		var ledgerName = "check-revert-then-meta"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transaction
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user", big.NewInt(1000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Add metadata before revert
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(ledgerName, txID, map[string]string{
						"pre-revert": "note",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Revert
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					revertTransactionAction(ledgerName, txID, false, false, nil),
				},
			})
			Expect(err).To(Succeed())

			// Add metadata after revert
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(ledgerName, txID, map[string]string{
						"post-revert": "reason",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After bulk operations in a single Apply call", Ordered, func() {
		var ledgerName = "check-bulk"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Bulk: multiple transactions + metadata operations in single request
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "user:1", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "user:2", big.NewInt(200), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "user:3", big.NewInt(300), "USD"),
					}, nil, nil),
					saveAccountMetadataAction(ledgerName, "bank", map[string]string{
						"type": "main-bank",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("After many reversions in sequence", Ordered, func() {
		var ledgerName = "check-many-reverts"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create and immediately revert multiple transactions
			for i := 1; i <= 5; i++ {
				resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createForceTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("temp:%d", i), big.NewInt(int64(i*100)), "USD"),
						}, nil),
					},
				})
				Expect(err).To(Succeed())

				txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

				_, err = client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						revertTransactionAction(ledgerName, txID, false, false, map[string]string{
							"batch": fmt.Sprintf("%d", i),
						}),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should pass integrity check", func() {
			expectStoreValid(ctx, client)
		})
	})

	Context("Comprehensive mixed workload", Ordered, func() {
		// This test exercises every type of operation through the full gRPC stack
		// and verifies the store remains consistent.
		BeforeAll(func() {
			// 1. Create ledgers with various configurations
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("check-comprehensive-trading", nil),
					createLedgerAction("check-comprehensive-payments", map[string]string{
						"region": "eu", "currency": "EUR",
					}),
				},
			})
			Expect(err).To(Succeed())

			trading := "check-comprehensive-trading"
			payments := "check-comprehensive-payments"

			// 2. Fund multiple accounts in multiple assets across ledgers
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(trading, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(500000), "USD"),
					}, nil),
					createForceTransactionAction(trading, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(400000), "EUR"),
					}, nil),
					createForceTransactionAction(trading, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(300000), "GBP"),
					}, nil),
					createForceTransactionAction(payments, []*commonpb.Posting{
						newPosting("world", "escrow", big.NewInt(100000), "EUR"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// 3. Multi-posting distribution
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(trading, []*commonpb.Posting{
						newPosting("bank", "trader:alpha", big.NewInt(10000), "USD"),
						newPosting("bank", "trader:beta", big.NewInt(20000), "USD"),
						newPosting("bank", "trader:gamma", big.NewInt(5000), "EUR"),
					}, map[string]string{"batch": "distribution-1"}, nil),
				},
			})
			Expect(err).To(Succeed())

			// 4. Inter-account transfers
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(trading, []*commonpb.Posting{
						newPosting("trader:alpha", "trader:beta", big.NewInt(1000), "USD"),
					}, nil, nil),
					createTransactionAction(trading, []*commonpb.Posting{
						newPosting("trader:beta", "trader:gamma", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// 5. Account metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(trading, "trader:alpha", map[string]string{
						"strategy": "aggressive", "level": "pro",
					}),
					saveAccountMetadataAction(trading, "trader:beta", map[string]string{
						"strategy": "conservative",
					}),
					saveAccountMetadataAction(payments, "escrow", map[string]string{
						"type": "escrow", "locked": "true",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 6. Transaction metadata
			// Get a transaction ID for metadata
			txResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(payments, []*commonpb.Posting{
						newPosting("escrow", "merchant:1", big.NewInt(5000), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			paymentTxID := txResp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(payments, paymentTxID, map[string]string{
						"invoice": "INV-001", "approved_by": "admin",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 7. Revert a transaction
			revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(trading, []*commonpb.Posting{
						newPosting("world", "trader:delta", big.NewInt(999), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			revertTargetID := revertResp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					revertTransactionAction(trading, revertTargetID, false, false, map[string]string{
						"reason": "error correction",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 8. Delete metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					deleteAccountMetadataAction(trading, "trader:alpha", "level"),
					deleteTransactionMetadataAction(payments, paymentTxID, "approved_by"),
				},
			})
			Expect(err).To(Succeed())

			// 9. More operations after all the above
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(trading, []*commonpb.Posting{
						newPosting("bank", "trader:alpha", big.NewInt(777), "GBP"),
					}, nil, nil),
					saveAccountMetadataAction(trading, "trader:alpha", map[string]string{
						"level": "elite",
					}),
				},
			})
			Expect(err).To(Succeed())

			// 10. Numscript transaction
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(trading, `
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
			expectStoreValid(ctx, client)
		})

		It("Should report progress correctly", func() {
			_, progress, err := collectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())

			Expect(progress).NotTo(BeEmpty())
			last := progress[len(progress)-1]
			Expect(last.LogsChecked).To(Equal(last.TotalLogs))
			// We created many logs across all the operations
			Expect(last.TotalLogs).To(BeNumerically(">=", 15))
		})
	})
})
