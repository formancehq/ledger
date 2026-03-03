//go:build e2e

package e2e

import (
	"context"
	"fmt"
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

var _ = Describe("Numscript", Ordered, func() {
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

	Context("When creating transactions with Numscript", Ordered, func() {
		var ledgerName = "numscript-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"source":      "world",
						"destination": "bank",
						"amount":      "USD/2 1000",
					}, nil),
				},
			})
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
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"destination": "users:alice",
						"amount":      "EUR/2 5000",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account balance
			Eventually(func(g Gomega) {
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, `
send [USD/2 10000] (
  source = @world
  destination = @sales:revenue
)
`, nil, nil),
				},
			})
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"source":       "sales:revenue",
						"tax_account":  "taxes:vat",
						"main_account": "bank:main",
						"amount":       "USD/2 1000",
					}, nil),
				},
			})
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
				taxAccount, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "taxes:vat",
				})
				g.Expect(err).To(Succeed())
				g.Expect(taxAccount.Volumes).To(HaveKey("USD/2"))
				g.Expect(taxAccount.Volumes["USD/2"].Balance).To(Equal("200")) // 20% of 1000
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				mainAccount, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, `
send [USD/2 50] (
  source = @world
  destination = @users:bob:wallet
)
`, nil, nil),
					createScriptTransactionAction(ledgerName, `
send [USD/2 200] (
  source = @world
  destination = @users:bob:bank
)
`, nil, nil),
				},
			})
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"wallet":      "users:bob:wallet",
						"bank":        "users:bob:bank",
						"destination": "merchants:shop",
						"amount":      "USD/2 150",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify account balances
			Eventually(func(g Gomega) {
				wallet, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:bob:wallet",
				})
				g.Expect(err).To(Succeed())
				g.Expect(wallet.Volumes).To(HaveKey("USD/2"))
				g.Expect(wallet.Volumes["USD/2"].Balance).To(Equal("0")) // Fully drained
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				bank, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:bob:bank",
				})
				g.Expect(err).To(Succeed())
				g.Expect(bank.Volumes).To(HaveKey("USD/2"))
				g.Expect(bank.Volumes["USD/2"].Balance).To(Equal("100")) // 200 - 100 (remainder)
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				shop, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, `
send [EUR/2 100] (
  source = @world
  destination = @users:charlie
)
`, nil, nil),
				},
			})
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"source":      "users:charlie",
						"destination": "merchants:store",
						"amount":      "EUR/2 400",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify charlie's balance is now negative (-300 = 100 - 400)
			Eventually(func(g Gomega) {
				charlie, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, `
send [USD/2 100] (
  source = @world
  destination = @users:dave
)
`, nil, nil),
				},
			})
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
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"source":      "users:dave",
						"destination": "merchants:store",
						"amount":      "USD/2 500", // 100 balance + 200 overdraft = 300 max, but we try 500
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Not enough funds"))
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"credit_line": "credit:eve",
						"destination": "bank:main",
						"amount":      "USD/2 100000",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify credit line is negative
			Eventually(func(g Gomega) {
				creditLine, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"buyer":  "users:frank",
						"seller": "merchants:gadgets",
						"amount": "USD/2 299",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify transaction metadata
			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Metadata).NotTo(BeNil())
			metaMap := createdTx.Transaction.Metadata.ToMap()
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"destination": "users:grace:savings",
						"amount":      "USD/2 1000",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify account metadata
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "users:grace:savings",
			})
			Expect(err).To(Succeed())
			metaMap := account.Metadata.ToMap()
			Expect(metaMap["account_type"]).To(Equal("savings"))
			Expect(metaMap["created_by"]).To(Equal("numscript"))
		})

		It("Should create a transaction with dynamic account address", func() {
			script := `
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, `
send [USD/2 1000] (
  source = @world
  destination = @users:henry
)
`, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Create escrow transaction with dynamic address
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"buyer":    "users:henry",
						"order_id": "order-12345",
						"amount":   "USD/2 500",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the escrow account was created with the dynamic address
			escrow, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := extractGRPCErrorInfo(err)
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"source": "world",
						// missing "destination" and "amount"
					}, nil),
				},
			})
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"destination": "bulk:account1",
						"amount":      "USD/2 100",
					}, nil),
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"destination": "bulk:account2",
						"amount":      "USD/2 200",
					}, nil),
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"destination": "bulk:account3",
						"amount":      "USD/2 300",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(3))

			// Verify all accounts
			for i, expected := range []string{"100", "200", "300"} {
				address := fmt.Sprintf("bulk:account%d", i+1)
				expectedBalance := expected
				Eventually(func(g Gomega) {
					account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
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

	// Tests for Numscript metadata preload: scripts that read account metadata
	// via meta() must have that metadata preloaded into the cache by the admission layer.
	Context("When using Numscript with metadata queries (meta())", Ordered, func() {
		var ledgerName = "numscript-meta-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should route funds using meta() to read destination from account metadata", func() {
			// Step 1: Set metadata on a routing account to define where funds should go
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "routing:orders", map[string]string{
						"destination": "merchant:shop1",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Step 2: Execute a Numscript that reads the destination from metadata
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"amount": "USD/2 5000",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the posting was routed to the metadata-specified destination
			log := resp.Logs[0]
			createdTx := log.Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("world"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("merchant:shop1"))
			Expect(createdTx.Transaction.Postings[0].Asset).To(Equal("USD/2"))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("5000"))
		})

		It("Should chain meta() calls to resolve nested metadata-driven routing", func() {
			// Step 1: Set up a chain: sale account → seller account → commission rate
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "sale:order-42", map[string]string{
						"seller": "sellers:acme",
					}),
					saveAccountMetadataAction(ledgerName, "sellers:acme", map[string]string{
						"commission": "15/100",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Step 2: Execute a script that reads seller from sale metadata,
			// then reads commission from seller metadata
			script := `
vars {
  account $sale
  account $seller = meta($sale, "seller")
  portion $commission = meta($seller, "commission")
}

send [USD/2 10000] (
  source = @world
  destination = {
    $commission to @platform:fees
    remaining to $seller
  }
)
`
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, map[string]string{
						"sale": "sale:order-42",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the split: 15% to platform, 85% to seller
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))

			// Find the posting to platform:fees (commission)
			var platformPosting, sellerPosting *commonpb.Posting
			for _, p := range createdTx.Transaction.Postings {
				switch p.Destination {
				case "platform:fees":
					platformPosting = p
				case "sellers:acme":
					sellerPosting = p
				}
			}
			Expect(platformPosting).NotTo(BeNil(), "should have a posting to platform:fees")
			Expect(sellerPosting).NotTo(BeNil(), "should have a posting to sellers:acme")

			// 15% of 10000 = 1500
			Expect(platformPosting.Amount.ToBigInt().Cmp(big.NewInt(1500))).To(Equal(0))
			// 85% of 10000 = 8500
			Expect(sellerPosting.Amount.ToBigInt().Cmp(big.NewInt(8500))).To(Equal(0))
		})

		It("Should handle meta() when metadata was set by a previous Numscript execution", func() {
			// Step 1: Use set_account_meta in a first Numscript to write metadata
			setupScript := `
set_account_meta(@config:treasury, "target", "treasury:main")

send [EUR/2 1000] (
  source = @world
  destination = @config:treasury
)
`
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, setupScript, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Step 2: Use meta() in a second Numscript to read the metadata written above
			routeScript := `
vars {
  account $dest = meta(@config:treasury, "target")
  monetary $amount
}

send $amount (
  source = @world
  destination = $dest
)
`
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, routeScript, map[string]string{
						"amount": "EUR/2 2000",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("treasury:main"))
		})

		It("Should preload metadata correctly after many transactions (cache pressure)", func() {
			// Step 1: Set metadata on an account
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "routing:pressure-test", map[string]string{
						"target": "vault:pressure",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Step 2: Generate many transactions to increase the Raft index and potentially
			// trigger cache generation rotations, which would evict the metadata from cache
			for i := range 50 {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("pressure:account%d", i), big.NewInt(100), "USD/2"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Step 3: Now execute a Numscript that reads the metadata set in step 1.
			// Without the metadata preload fix, this would see empty metadata because
			// the cache generation containing the metadata may have been rotated out.
			script := `
vars {
  account $dest = meta(@routing:pressure-test, "target")
}

send [USD/2 999] (
  source = @world
  destination = $dest
)
`
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("vault:pressure"),
				"metadata should be correctly preloaded even after many transactions")
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("999"))
		})

		It("Should handle updated metadata correctly with meta()", func() {
			// Step 1: Set initial routing metadata
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "routing:mutable", map[string]string{
						"destination": "old:target",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Step 2: Update the metadata to point elsewhere
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "routing:mutable", map[string]string{
						"destination": "new:target",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Step 3: Numscript should see the updated metadata
			script := `
vars {
  account $dest = meta(@routing:mutable, "destination")
}

send [GBP/2 3000] (
  source = @world
  destination = $dest
)
`
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptTransactionAction(ledgerName, script, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("new:target"),
				"should see updated metadata, not stale value")
		})
	})
})
