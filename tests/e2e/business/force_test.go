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

var _ = Describe("Force Transactions", Ordered, func() {

	Context("When creating transactions with force=true", Ordered, func() {
		var ledgerName = "force-tx-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should allow transaction with insufficient funds when force=true", func() {
			// First, fund the account with a small amount
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "limited-account", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Verify the account has 100 USD
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "limited-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Balance).To(Equal("100"))

			// Try to send more than available without force - should fail
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("limited-account", "destination", big.NewInt(500), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("insufficient"))

			// Now try with force=true - should succeed
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("limited-account", "destination", big.NewInt(500), "USD"),
					}, nil),
				),
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("empty-account", "zero-dest", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(HaveOccurred())

			// With force=true - should succeed
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("empty-account", "zero-dest", big.NewInt(100), "USD"),
					}, nil),
				),
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// The source account should have negative balance
			sourceAccount, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "empty-account",
			})
			Expect(err).To(Succeed())
			Expect(sourceAccount.Volumes["USD"].Balance).To(Equal("-100"))

			// The destination account should have positive balance
			destAccount, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "zero-dest",
			})
			Expect(err).To(Succeed())
			Expect(destAccount.Volumes["USD"].Balance).To(Equal("100"))
		})

		It("Should create multiple postings with force=true", func() {
			// Multiple postings from accounts with no balance
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("source-a", "dest-1", big.NewInt(100), "USD"),
						actions.NewPosting("source-b", "dest-2", big.NewInt(200), "EUR"),
						actions.NewPosting("source-c", "dest-3", big.NewInt(300), "GBP"),
					}, nil),
				),
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
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
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

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("meta-source", "meta-dest", big.NewInt(100), "USD"),
					}, metadata),
				),
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify metadata in transaction
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Metadata).NotTo(BeNil())
			Expect(commonpb.MetadataToGoMap(createdTx.Transaction.Metadata)["description"]).To(Equal("Forced transaction"))
			Expect(commonpb.MetadataToGoMap(createdTx.Transaction.Metadata)["reason"]).To(Equal("bulk import"))
		})

		It("Should handle bulk transactions with mixed force flags", func() {
			// First fund an account for the non-force transaction
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "funded-account", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Bulk with a normal transaction (has funds) and a force transaction (no funds)
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					// Normal transaction - should succeed because account has funds
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("funded-account", "recipient-1", big.NewInt(500), "USD"),
					}, nil, nil),
					// Force transaction - should succeed despite no funds
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("unfunded-account", "recipient-2", big.NewInt(500), "USD"),
					}, nil),
				),
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})
	})

	Context("When using Numscript with force=true", Ordered, func() {
		var ledgerName = "force-numscript-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should allow Numscript transaction with insufficient funds when force=true", func() {
			// Without force, this would fail because users:broke has no balance
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceScriptTransactionAction(ledgerName, `
						send [USD/2 100000] (
							source = @users:broke
							destination = @users:alice
						)
					`, nil, nil),
				),
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
			Expect(posting.Amount.ToBigInt().Int64()).To(Equal(int64(100000)))
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

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceScriptTransactionAction(ledgerName, script, vars, nil),
				),
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
			Expect(posting.Amount.ToBigInt().Int64()).To(Equal(int64(50000)))
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

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceScriptTransactionAction(ledgerName, script, nil, nil),
				),
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

	Context("Verifying volumes are correctly tracked after force transactions", Ordered, func() {
		var ledgerName = "force-volumes-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
			})
			Expect(err).To(Succeed())
		})

		It("Should correctly track negative balance after force transaction", func() {
			// Force transaction from empty account
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("empty-source", "target", big.NewInt(500), "USD"),
					}, nil),
				),
			})
			Expect(err).To(Succeed())

			// Check source account has negative balance
			source, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "empty-source",
			})
			Expect(err).To(Succeed())
			Expect(source.Volumes["USD"].Input).To(Equal("0"))
			Expect(source.Volumes["USD"].Output).To(Equal("500"))
			Expect(source.Volumes["USD"].Balance).To(Equal("-500"))

			// Check target account has positive balance
			target, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
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
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Envelopes: servicepb.UnsignedEnvelopes(
						actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("debt-source", "receiver", big.NewInt(100), "USD"),
						}, nil),
					),
				})
				Expect(err).To(Succeed())
			}

			// Check accumulated debt
			source, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "debt-source",
			})
			Expect(err).To(Succeed())
			Expect(source.Volumes["USD"].Output).To(Equal("300"))
			Expect(source.Volumes["USD"].Balance).To(Equal("-300"))
		})

		It("Should allow force transactions to recover from negative balance", func() {
			// First, create debt with force
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("recovery-account", "some-dest", big.NewInt(500), "USD"),
					}, nil),
				),
			})
			Expect(err).To(Succeed())

			// Check negative balance
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "recovery-account",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Balance).To(Equal("-500"))

			// Fund the account to recover
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "recovery-account", big.NewInt(1000), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())

			// Check balance is now positive (1000 - 500 = 500)
			account, err = sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
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
