//go:build e2e

package business

import (
	"fmt"
	"time"

	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Numscript", Ordered, func() {

	Context("When creating transactions with Numscript", Ordered, func() {
		var ledgerName = "numscript-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should create a simple transaction with Numscript", func() {
			script := `
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = $source
  destination = $destination
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"source":      "world",
				"destination": "bank",
				"amount":      "USD/2 1000",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify transaction details
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("world"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("bank"))
			Expect(createdTx.Transaction.Postings[0].Asset).To(Equal("USD/2"))
			// Verify the posting amount is correct
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("1000"))

			// Verify account balance (use Eventually to handle potential timing issues)
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "bank",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.Volumes).To(HaveKey("USD/2"))
				g.Expect(account.Volumes["USD/2"].Balance).To(Equal("1000"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should create a transaction with world source (unbounded)", func() {
			script := `
vars {
  account $destination
  monetary $amount
}

send $amount (
  source = @world
  destination = $destination
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"destination": "users:alice",
				"amount":      "EUR/2 5000",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account balance
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:alice",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.Volumes).To(HaveKey("EUR/2"))
				g.Expect(account.Volumes["EUR/2"].Balance).To(Equal("5000"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should create a transaction with multiple destinations (percentage split)", func() {
			// First fund the source account
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 10000] (
  source = @world
  destination = @sales:revenue
)
`, nil, nil)))
			Expect(err).To(Succeed())

			// Now split the payment
			script := `
vars {
  account $source
  account $tax_account
  account $main_account
  monetary $amount
}

send $amount (
  source = $source
  destination = {
    20% to $tax_account
    remaining to $main_account
  }
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"source":       "sales:revenue",
				"tax_account":  "taxes:vat",
				"main_account": "bank:main",
				"amount":       "USD/2 1000",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify transaction has 2 postings (20% tax + 80% main)
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))

			// Verify account balances
			Eventually(func(g Gomega) {
				taxAccount, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "taxes:vat",
				})
				g.Expect(err).To(Succeed())
				g.Expect(taxAccount.Volumes).To(HaveKey("USD/2"))
				g.Expect(taxAccount.Volumes["USD/2"].Balance).To(Equal("200")) // 20% of 1000
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				mainAccount, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "bank:main",
				})
				g.Expect(err).To(Succeed())
				g.Expect(mainAccount.Volumes).To(HaveKey("USD/2"))
				g.Expect(mainAccount.Volumes["USD/2"].Balance).To(Equal("800")) // 80% of 1000
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should create a transaction with multiple sources (fallback)", func() {
			// Fund the wallet and bank accounts
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 50] (
  source = @world
  destination = @users:bob:wallet
)
`, nil, nil),
				actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 200] (
  source = @world
  destination = @users:bob:bank
)
`, nil, nil)))
			Expect(err).To(Succeed())

			// Now pay from multiple sources
			script := `
vars {
  account $wallet
  account $bank
  account $destination
  monetary $amount
}

send $amount (
  source = {
    $wallet
    $bank
  }
  destination = $destination
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"wallet":      "users:bob:wallet",
				"bank":        "users:bob:bank",
				"destination": "merchants:shop",
				"amount":      "USD/2 150",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account balances
			Eventually(func(g Gomega) {
				wallet, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:bob:wallet",
				})
				g.Expect(err).To(Succeed())
				g.Expect(wallet.Volumes).To(HaveKey("USD/2"))
				g.Expect(wallet.Volumes["USD/2"].Balance).To(Equal("0")) // Fully drained
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				bank, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:bob:bank",
				})
				g.Expect(err).To(Succeed())
				g.Expect(bank.Volumes).To(HaveKey("USD/2"))
				g.Expect(bank.Volumes["USD/2"].Balance).To(Equal("100")) // 200 - 100 (remainder)
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				shop, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "merchants:shop",
				})
				g.Expect(err).To(Succeed())
				g.Expect(shop.Volumes).To(HaveKey("USD/2"))
				g.Expect(shop.Volumes["USD/2"].Balance).To(Equal("150"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should create a transaction with bounded overdraft", func() {
			// Fund the account with some initial balance
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
send [EUR/2 100] (
  source = @world
  destination = @users:charlie
)
`, nil, nil)))
			Expect(err).To(Succeed())

			// Allow overdraft up to 500
			script := `
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = {
    $source allowing overdraft up to [EUR/2 500]
  }
  destination = $destination
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"source":      "users:charlie",
				"destination": "merchants:store",
				"amount":      "EUR/2 400",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify charlie's balance is now negative (-300 = 100 - 400)
			Eventually(func(g Gomega) {
				charlie, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:charlie",
				})
				g.Expect(err).To(Succeed())
				g.Expect(charlie.Volumes).To(HaveKey("EUR/2"))
				g.Expect(charlie.Volumes["EUR/2"].Balance).To(Equal("-300"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should fail when overdraft limit is exceeded", func() {
			// Fund the account with some initial balance
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 100] (
  source = @world
  destination = @users:dave
)
`, nil, nil)))
			Expect(err).To(Succeed())

			// Try to overdraft more than allowed
			script := `
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = {
    $source allowing overdraft up to [USD/2 200]
  }
  destination = $destination
)
`
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"source":      "users:dave",
				"destination": "merchants:store",
				"amount":      "USD/2 500", // 100 balance + 200 overdraft = 300 max, but we try 500
			}, nil)))
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInsufficientFunds))
		})

		It("Should create a transaction with unbounded overdraft", func() {
			script := `
vars {
  account $credit_line
  account $destination
  monetary $amount
}

send $amount (
  source = {
    $credit_line allowing unbounded overdraft
  }
  destination = $destination
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"credit_line": "credit:eve",
				"destination": "bank:main",
				"amount":      "USD/2 100000",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify credit line is negative
			Eventually(func(g Gomega) {
				creditLine, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "credit:eve",
				})
				g.Expect(err).To(Succeed())
				g.Expect(creditLine.Volumes).To(HaveKey("USD/2"))
				g.Expect(creditLine.Volumes["USD/2"].Balance).To(Equal("-100000"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should create a transaction with set_tx_meta", func() {
			script := `
vars {
  account $buyer
  account $seller
  monetary $amount
}

set_tx_meta("type", "payment")
set_tx_meta("category", "purchase")

send $amount (
  source = @world
  destination = $seller
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"buyer":  "users:frank",
				"seller": "merchants:gadgets",
				"amount": "USD/2 299",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify transaction metadata
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Metadata).NotTo(BeNil())
			metaMap := commonpb.MetadataToGoMap(createdTx.Transaction.Metadata)
			Expect(metaMap["type"]).To(Equal("payment"))
			Expect(metaMap["category"]).To(Equal("purchase"))
		})

		It("Should create a transaction with set_account_meta", func() {
			script := `
vars {
  account $destination
  monetary $amount
}

set_account_meta($destination, "account_type", "savings")
set_account_meta($destination, "created_by", "numscript")

send $amount (
  source = @world
  destination = $destination
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"destination": "users:grace:savings",
				"amount":      "USD/2 1000",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify account metadata
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "users:grace:savings",
			})
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(account.Metadata)
			Expect(metaMap["account_type"]).To(Equal("savings"))
			Expect(metaMap["created_by"]).To(Equal("numscript"))
		})

		It("Should create a transaction with dynamic account address", func() {
			script := `
#![feature("experimental-account-interpolation")]

vars {
  account $buyer
  string $order_id
  monetary $amount
}

send $amount (
  source = $buyer
  destination = @escrow:$order_id
)
`
			// First fund the buyer
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 1000] (
  source = @world
  destination = @users:henry
)
`, nil, nil)))
			Expect(err).To(Succeed())

			// Create escrow transaction with dynamic address
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"buyer":    "users:henry",
				"order_id": "order-12345",
				"amount":   "USD/2 500",
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the escrow account was created with the dynamic address
			escrow, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "escrow:order-12345",
			})
			Expect(err).To(Succeed())
			Expect(escrow.Volumes["USD/2"].Balance).To(Equal("500"))
		})

		It("Should fail with invalid Numscript syntax", func() {
			script := `
send [USD/2 100] (
  source = @world
  destination = // missing destination
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptParseError))
			Expect(info.Domain).To(Equal("ledger"))
		})

		It("Should fail with missing variable", func() {
			script := `
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = $source
  destination = $destination
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"source": "world",
				// missing "destination" and "amount"
			}, nil)))
			Expect(err).To(HaveOccurred())
		})

		It("Should create multiple transactions with Numscript in bulk", func() {
			script := `
vars {
  account $destination
  monetary $amount
}

send $amount (
  source = @world
  destination = $destination
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"destination": "bulk:account1",
				"amount":      "USD/2 100",
			}, nil),
				actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
					"destination": "bulk:account2",
					"amount":      "USD/2 200",
				}, nil),
				actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
					"destination": "bulk:account3",
					"amount":      "USD/2 300",
				}, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(3))

			// Verify all accounts
			for i, expected := range []string{"100", "200", "300"} {
				address := fmt.Sprintf("bulk:account%d", i+1)
				expectedBalance := expected
				Eventually(func(g Gomega) {
					account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
						Ledger:  ledgerName,
						Address: address,
					})
					g.Expect(err).To(Succeed())
					g.Expect(account.Volumes).To(HaveKey("USD/2"))
					g.Expect(account.Volumes["USD/2"].Balance).To(Equal(expectedBalance))
				}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
			}
		})
	})

	// meta() is not supported — scripts must use static account references.
	Context("When using Numscript with meta() calls", func() {
		It("Should reject scripts that use meta() to resolve variables", func() {
			ledgerName := "numscript-meta-rejected"
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			script := `
vars {
  account $dest = meta(@routing:orders, "destination")
  monetary $amount
}

send $amount (
  source = @world
  destination = $dest
)
`
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
				"amount": "USD/2 5000",
			}, nil)))
			Expect(err).To(HaveOccurred())
			Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
		})
	})

	// Regression coverage for ledger#1500: overdraft() folds to zero against
	// emulation's fake positive balance, so `send overdraft(...)` produced no
	// posting during dependency discovery, and the unbounded-overdraft source's
	// volume was never preloaded. At apply the real overdraft was non-zero, the
	// posting materialised, and the write hit "read of undeclared key"
	// (STORAGE_OPERATION_FAILED). The fix unions numscript's static
	// GetInvolvedAccounts analysis into the preload set. These tests exercise
	// the fix end-to-end through the gRPC apply path.
	Context("When repaying an unbounded overdraft via overdraft() (regression #1500)", Ordered, func() {
		var ledgerName = "numscript-overdraft-1500"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should repay via a mid-script overdraft() call from an unbounded-overdraft source", func() {
			// Step 1: put @credit into overdraft by 100. This mirrors the reproduction
			// in the issue and matches emulation's expectations (positive fake balance
			// on @main allows the send, no cross-account read on @credit yet).
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 100] (
  source = @credit allowing unbounded overdraft
  destination = @main
)
`, nil, nil)))
			Expect(err).To(Succeed())

			// Step 2: repay the overdraft. Before the fix this failed with
			// STORAGE_OPERATION_FAILED / "source volume repay/USD/2" because @repay
			// was never preloaded. With the static-involved-accounts union, @repay is
			// preloaded and the transaction applies cleanly.
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
#![feature("experimental-overdraft-function", "experimental-mid-script-function-call")]
send overdraft(@credit, USD/2) (
  source = @repay allowing unbounded overdraft
  destination = @credit
)
`, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// The repay posting materialises for exactly the overdrawn amount.
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("repay"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("credit"))
			Expect(createdTx.Transaction.Postings[0].Asset).To(Equal("USD/2"))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("100"))

			// @credit is now flat (was -100, received +100 from @repay).
			Eventually(func(g Gomega) {
				credit, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "credit",
				})
				g.Expect(err).To(Succeed())
				g.Expect(credit.Volumes).To(HaveKey("USD/2"))
				g.Expect(credit.Volumes["USD/2"].Balance).To(Equal("0"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			// @repay took the debt via its unbounded overdraft.
			Eventually(func(g Gomega) {
				repay, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "repay",
				})
				g.Expect(err).To(Succeed())
				g.Expect(repay.Volumes).To(HaveKey("USD/2"))
				g.Expect(repay.Volumes["USD/2"].Balance).To(Equal("-100"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should repay when overdraft() is bound through a vars block", func() {
			// The issue notes: "Binding overdraft() through a vars block instead of a
			// mid-script call fails identically." Same emulation gap, different call
			// site — this locks the vars-block variant in as a regression.
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 50] (
  source = @credit_vars allowing unbounded overdraft
  destination = @main
)
`, nil, nil)))
			Expect(err).To(Succeed())

			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptTransactionAction(ledgerName, `
#![feature("experimental-overdraft-function")]
vars {
  monetary $due = overdraft(@credit_vars, USD/2)
}
send $due (
  source = @repay_vars allowing unbounded overdraft
  destination = @credit_vars
)
`, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("repay_vars"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("credit_vars"))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("50"))

			Eventually(func(g Gomega) {
				credit, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "credit_vars",
				})
				g.Expect(err).To(Succeed())
				g.Expect(credit.Volumes["USD/2"].Balance).To(Equal("0"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				repay, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "repay_vars",
				})
				g.Expect(err).To(Succeed())
				g.Expect(repay.Volumes["USD/2"].Balance).To(Equal("-50"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})
	})
})
