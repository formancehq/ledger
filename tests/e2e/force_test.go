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

var _ = Describe("Force Transactions", func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = 9500
		grpcPort = 8500
	)

	BeforeEach(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("When creating transactions with force=true", func() {
		var ledgerName = "force-tx-ledger"

		BeforeEach(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow transaction with insufficient funds when force=true", func() {
			// First, fund the account with a small amount
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "limited-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify the account has 100 USD
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "limited-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Balance).To(Equal("100"))

			// Try to send more than available without force - should fail
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("limited-account", "destination", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("insufficient"))

			// Now try with force=true - should succeed
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("limited-account", "destination", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the transaction was created
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
		})

		It("Should allow transaction from account with zero balance when force=true", func() {
			// Account with zero balance - should fail without force
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("empty-account", "destination", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			// With force=true - should succeed
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("empty-account", "destination", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// The source account should have negative balance
			sourceAccount, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "empty-account",
			})
			Expect(err).To(Succeed())
			Expect(sourceAccount.Volumes["USD"].Balance).To(Equal("-100"))

			// The destination account should have positive balance
			destAccount, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "destination",
			})
			Expect(err).To(Succeed())
			Expect(destAccount.Volumes["USD"].Balance).To(Equal("100"))
		})

		It("Should create multiple postings with force=true", func() {
			// Multiple postings from accounts with no balance
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("source-a", "dest-1", big.NewInt(100), "USD"),
						newPosting("source-b", "dest-2", big.NewInt(200), "EUR"),
						newPosting("source-c", "dest-3", big.NewInt(300), "GBP"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify all source accounts have negative balances
			for _, tc := range []struct {
				addr    string
				asset   string
				balance string
			}{
				{"source-a", "USD", "-100"},
				{"source-b", "EUR", "-200"},
				{"source-c", "GBP", "-300"},
			} {
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: tc.addr,
				})
				Expect(err).To(Succeed())
				Expect(account.Volumes[tc.asset].Balance).To(Equal(tc.balance))
			}
		})

		It("Should work with metadata when force=true", func() {
			metadata := map[string]string{
				"description": "Forced transaction",
				"reason":      "bulk import",
			}

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("meta-source", "meta-dest", big.NewInt(100), "USD"),
					}, metadata),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify metadata in transaction
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Metadata).NotTo(BeNil())
			Expect(createdTx.Transaction.Metadata.ToMap()["description"]).To(Equal("Forced transaction"))
			Expect(createdTx.Transaction.Metadata.ToMap()["reason"]).To(Equal("bulk import"))
		})

		It("Should handle bulk transactions with mixed force flags", func() {
			// First fund an account for the non-force transaction
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "funded-account", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Bulk with a normal transaction (has funds) and a force transaction (no funds)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					// Normal transaction - should succeed because account has funds
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("funded-account", "recipient-1", big.NewInt(500), "USD"),
					}, nil, nil),
					// Force transaction - should succeed despite no funds
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("unfunded-account", "recipient-2", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})
	})

	Context("When using Numscript with force=true", func() {
		var ledgerName = "force-numscript-ledger"

		BeforeEach(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow Numscript transaction with insufficient funds when force=true", func() {
			// Without force, this would fail because users:broke has no balance
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceScriptTransactionAction(ledgerName, `
						send [USD/2 100000] (
							source = @users:broke
							destination = @users:alice
						)
					`, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the transaction was created with correct postings
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))

			posting := createdTx.Transaction.Postings[0]
			Expect(posting.Source).To(Equal("users:broke"))
			Expect(posting.Destination).To(Equal("users:alice"))
			Expect(posting.Amount.Value().Int64()).To(Equal(int64(100000)))
		})

		It("Should allow Numscript with variables when force=true", func() {
			script := `
				vars {
					monetary $amount
					account $source
				}
				send $amount (
					source = $source
					destination = @destination:account
				)
			`
			vars := map[string]string{
				"amount": "EUR/2 50000",
				"source": "source:account",
			}

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceScriptTransactionAction(ledgerName, script, vars, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			posting := createdTx.Transaction.Postings[0]
			Expect(posting.Source).To(Equal("source:account"))
			Expect(posting.Destination).To(Equal("destination:account"))
			Expect(posting.Amount.Value().Int64()).To(Equal(int64(50000)))
			Expect(posting.Asset).To(Equal("EUR/2"))
		})

		It("Should work with multiple send statements in Numscript when force=true", func() {
			script := `
				send [USD/2 10000] (
					source = @empty:a
					destination = @users:alice
				)
				send [EUR/2 20000] (
					source = @empty:b
					destination = @users:bob
				)
			`

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceScriptTransactionAction(ledgerName, script, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))
		})
	})

	Context("Verifying volumes are correctly tracked after force transactions", func() {
		var ledgerName = "force-volumes-ledger"

		BeforeEach(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should correctly track negative balance after force transaction", func() {
			// Force transaction from empty account
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("empty-source", "target", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Check source account has negative balance
			source, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "empty-source",
			})
			Expect(err).To(Succeed())
			Expect(source.Volumes["USD"].Input).To(Equal("0"))
			Expect(source.Volumes["USD"].Output).To(Equal("500"))
			Expect(source.Volumes["USD"].Balance).To(Equal("-500"))

			// Check target account has positive balance
			target, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "target",
			})
			Expect(err).To(Succeed())
			Expect(target.Volumes["USD"].Input).To(Equal("500"))
			Expect(target.Volumes["USD"].Output).To(Equal("0"))
			Expect(target.Volumes["USD"].Balance).To(Equal("500"))
		})

		It("Should allow subsequent force transactions to accumulate debt", func() {
			// Multiple force transactions from the same empty account
			for i := 0; i < 3; i++ {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createForceTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("debt-source", "receiver", big.NewInt(100), "USD"),
						}, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Check accumulated debt
			source, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "debt-source",
			})
			Expect(err).To(Succeed())
			Expect(source.Volumes["USD"].Output).To(Equal("300"))
			Expect(source.Volumes["USD"].Balance).To(Equal("-300"))
		})

		It("Should allow force transactions to recover from negative balance", func() {
			// First, create debt with force
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("recovery-account", "some-dest", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Check negative balance
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "recovery-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Balance).To(Equal("-500"))

			// Fund the account to recover
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "recovery-account", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Check balance is now positive (1000 - 500 = 500)
			account, err = client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "recovery-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Input).To(Equal("1000"))
			Expect(account.Volumes["USD"].Output).To(Equal("500"))
			Expect(account.Volumes["USD"].Balance).To(Equal("500"))
		})
	})
})
