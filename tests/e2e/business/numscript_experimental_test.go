//go:build e2e

package business

import (
	"fmt"
	"sync"
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

// NumscriptExperimental exercises the EN-1406 dependency-resolution overhaul
// against a real server: dependency discovery now runs upstream
// ParseResult.ResolveDependencies (instead of emulation), admission binds the
// balance/metadata values that determined the resolution into
// CreateTransactionOrder.inputs_resolution_hash, and the FSM re-resolves against
// the coverage-gated cache. These specs cover the advanced / experimental
// Numscript surface that motivated the change and assert real outcomes
// (balances, postings, stored metadata), not just success.
var _ = Describe("NumscriptExperimental (EN-1406)", Ordered, func() {

	// getVolume reads back the balance of an account for an asset, retrying to
	// absorb read-side projection lag.
	getVolume := func(ledgerName, address, asset string) func(g Gomega) string {
		return func(g Gomega) string {
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: address,
			})
			g.Expect(err).To(Succeed())
			g.Expect(account.Volumes).To(HaveKey(asset))
			return account.Volumes[asset].Balance
		}
	}

	expectBalance := func(ledgerName, address, asset, want string) {
		Eventually(func(g Gomega) {
			g.Expect(getVolume(ledgerName, address, asset)(g)).To(Equal(want))
		}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	}

	fund := func(ledgerName, address, monetary string) {
		script := fmt.Sprintf(`send [%s] (source = @world destination = @%s)`, monetary, address)
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
		Expect(err).To(Succeed())
	}

	Context("oneof source selection (exhaustive dependency discovery)", Ordered, func() {
		const ledgerName = "nsx-oneof"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should select the first funded branch, with all branches discovered as deps", func() {
			// Only branch B is funded. oneof must skip the empty branch A and
			// pick B. If admission failed to preload every branch's volume,
			// apply would either mis-resolve or hit a coverage miss.
			fund(ledgerName, "oneof:b", "USD/2 1000")

			script := `
#![feature("experimental-oneof")]

send [USD/2 400] (
  source = oneof {
    @oneof:a
    @oneof:b
  }
  destination = @oneof:sink
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("oneof:b"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("oneof:sink"))

			expectBalance(ledgerName, "oneof:b", "USD/2", "600")
			expectBalance(ledgerName, "oneof:sink", "USD/2", "400")
		})

		It("Should pick a later branch when earlier branches are empty (dep on balance)", func() {
			// A is empty, C is funded: selection depends on reading A's balance
			// (zero) then falling through to C. Every branch's balance must have
			// been preloaded for this to resolve deterministically.
			fund(ledgerName, "oneof:c", "USD/2 250")

			script := `
#![feature("experimental-oneof")]

send [USD/2 250] (
  source = oneof {
    @oneof:empty
    @oneof:c
  }
  destination = @oneof:sink2
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("oneof:c"))

			expectBalance(ledgerName, "oneof:c", "USD/2", "0")
			expectBalance(ledgerName, "oneof:sink2", "USD/2", "250")
		})
	})

	Context("meta() driving resolution (variables and selectors)", Ordered, func() {
		const ledgerName = "nsx-meta"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should resolve BOTH source and destination from meta() reads", func() {
			// Seed routing metadata that meta() reads to determine both the
			// source and destination accounts. The resolved source must have
			// its volume preloaded (it is a balance-checked source).
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.SaveAccountMetadataAction(ledgerName, "routing:cfg", map[string]string{
					"src": "vault:main",
					"dst": "payouts:eur",
				})))
			Expect(err).To(Succeed())

			fund(ledgerName, "vault:main", "EUR/2 900")

			script := `
vars {
  account $src = meta(@routing:cfg, "src")
  account $dst = meta(@routing:cfg, "dst")
  monetary $amount
}

send $amount (
  source = $src
  destination = $dst
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, map[string]string{
					"amount": "EUR/2 350",
				}, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("vault:main"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("payouts:eur"))

			expectBalance(ledgerName, "vault:main", "EUR/2", "550")
			expectBalance(ledgerName, "payouts:eur", "EUR/2", "350")
		})
	})

	Context("balance() in variables and caps (CM-209 acceptance)", Ordered, func() {
		const ledgerName = "nsx-balance-fn"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should ACCEPT a balance()-derived cap (previously rejected as non-deterministic)", func() {
			// The overdraft bound is derived from a second account's balance,
			// read into a var: the script therefore reads two balances (payer +
			// reserve). The old emulation rejected multi-balance scripts as
			// NON_DETERMINISTIC_SCRIPT; ResolveDependencies records both reads so
			// it now succeeds. (balance() lives in vars, not inline in the cap
			// expression — inline mid-script calls need a separate experimental
			// flag and are not what CM-209 is about.)
			fund(ledgerName, "cm209:reserve", "USD/2 500")
			fund(ledgerName, "cm209:payer", "USD/2 100")

			script := `
vars {
  monetary $cap = balance(@cm209:reserve, USD/2)
}

send [USD/2 300] (
  source = @cm209:payer allowing overdraft up to $cap
  destination = @cm209:merchant
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// payer had 100, sends 300 => -200 (within the 500 overdraft bound).
			expectBalance(ledgerName, "cm209:payer", "USD/2", "-200")
			expectBalance(ledgerName, "cm209:merchant", "USD/2", "300")
		})

		It("Should use balance() in a variable to size a send", func() {
			// $half is derived from a balance; the send amount depends on a read
			// balance. Assert the derived amount posts correctly.
			fund(ledgerName, "cm209:vaultA", "USD/2 800")

			script := `
vars {
  monetary $bal = balance(@cm209:vaultA, USD/2)
}

send $bal (
  source = @cm209:vaultA
  destination = @cm209:drain
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("800"))

			expectBalance(ledgerName, "cm209:vaultA", "USD/2", "0")
			expectBalance(ledgerName, "cm209:drain", "USD/2", "800")
		})
	})

	Context("source inclusion depends on a non-zero balance (CM-206)", Ordered, func() {
		const ledgerName = "nsx-cm206"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should discover and drain a balance-checked source ahead of a fallback", func() {
			// wallet is funded; overflow is unbounded. The send drains wallet
			// first (its balance must be discovered/preloaded) and only overflows
			// into the credit line for the remainder.
			fund(ledgerName, "cm206:wallet", "USD/2 150")

			script := `
send [USD/2 400] (
  source = {
    @cm206:wallet
    @cm206:credit allowing unbounded overdraft
  }
  destination = @cm206:out
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			// wallet contributes 150, credit contributes 250.
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))

			expectBalance(ledgerName, "cm206:wallet", "USD/2", "0")
			expectBalance(ledgerName, "cm206:credit", "USD/2", "-250")
			expectBalance(ledgerName, "cm206:out", "USD/2", "400")
		})
	})

	Context("overdraft sources (bounded and unbounded)", Ordered, func() {
		const ledgerName = "nsx-overdraft"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should allow unbounded overdraft", func() {
			script := `
send [GBP/2 75000] (
  source = @od:credit allowing unbounded overdraft
  destination = @od:beneficiary
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			expectBalance(ledgerName, "od:credit", "GBP/2", "-75000")
			expectBalance(ledgerName, "od:beneficiary", "GBP/2", "75000")
		})

		It("Should allow bounded overdraft up to a literal cap and reject beyond it", func() {
			fund(ledgerName, "od:acct", "GBP/2 200")

			// Within the bound: 200 balance + 300 overdraft = 500 available.
			okScript := `
send [GBP/2 450] (
  source = @od:acct allowing overdraft up to [GBP/2 300]
  destination = @od:shop
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, okScript, nil, nil)))
			Expect(err).To(Succeed())
			expectBalance(ledgerName, "od:acct", "GBP/2", "-250")

			// Beyond the bound: now at -250, only -300 allowed, so sending 100
			// more (=> -350) must fail with insufficient funds.
			overScript := `
send [GBP/2 100] (
  source = @od:acct allowing overdraft up to [GBP/2 300]
  destination = @od:shop
)
`
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, overScript, nil, nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInsufficientFunds))
		})
	})

	Context("multi-send scripts (multiple send blocks)", Ordered, func() {
		const ledgerName = "nsx-multisend"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should execute multiple send blocks in one script atomically", func() {
			fund(ledgerName, "ms:hub", "USD/2 1000")

			// Three send blocks: two funded from world, one draining the hub.
			// All must post as postings of a single transaction.
			script := `
send [USD/2 1000] (
  source = @world
  destination = @ms:a
)

send [USD/2 500] (
  source = @world
  destination = @ms:b
)

send [USD/2 300] (
  source = @ms:hub
  destination = @ms:c
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(3))

			expectBalance(ledgerName, "ms:a", "USD/2", "1000")
			expectBalance(ledgerName, "ms:b", "USD/2", "500")
			expectBalance(ledgerName, "ms:hub", "USD/2", "700")
			expectBalance(ledgerName, "ms:c", "USD/2", "300")
		})
	})

	Context("metadata writes and the ValueToString contract", Ordered, func() {
		const ledgerName = "nsx-meta-writes"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should store set_tx_meta values as their unquoted string representation", func() {
			// set_tx_meta with string, number, monetary, account and asset
			// values. Ledger stores the raw client-facing string: string/number
			// unquoted (no JSON quotes), monetary as "ASSET amount", account with
			// a leading @, asset verbatim. This guards ValueToString.
			script := `
set_tx_meta("label", "gold-tier")
set_tx_meta("count", 42)
set_tx_meta("fee", [USD/2 150])
set_tx_meta("beneficiary", @merchants:acme)
set_tx_meta("currency", USD/2)

send [USD/2 100] (
  source = @world
  destination = @vts:dest
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			meta := commonpb.MetadataToGoMap(createdTx.Transaction.Metadata)

			Expect(meta["label"]).To(Equal("gold-tier"))             // string: unquoted
			Expect(meta["count"]).To(Equal("42"))                    // number: unquoted
			Expect(meta["fee"]).To(Equal("USD/2 150"))               // monetary: canonical
			Expect(meta["beneficiary"]).To(Equal("@merchants:acme")) // account: @-prefixed
			Expect(meta["currency"]).To(Equal("USD/2"))              // asset: verbatim
		})

		It("Should store set_account_meta values as their unquoted string representation", func() {
			script := `
set_account_meta(@vts:acct, "tier", "premium")
set_account_meta(@vts:acct, "score", 7)

send [USD/2 100] (
  source = @world
  destination = @vts:acct
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "vts:acct",
				})
				g.Expect(err).To(Succeed())
				meta := commonpb.MetadataToGoMap(account.Metadata)
				g.Expect(meta["tier"]).To(Equal("premium"))
				g.Expect(meta["score"]).To(Equal("7"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})
	})

	Context("portions and percentage splits", Ordered, func() {
		const ledgerName = "nsx-portions"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should split a send with a fractional portion and a percentage", func() {
			fund(ledgerName, "pt:src", "USD/2 1000")

			// 1/2 to fees, 25% to reserve, remainder to main.
			script := `
send [USD/2 1000] (
  source = @pt:src
  destination = {
    1/2 to @pt:fees
    25% to @pt:reserve
    remaining to @pt:main
  }
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(3))

			expectBalance(ledgerName, "pt:fees", "USD/2", "500")    // 1/2
			expectBalance(ledgerName, "pt:reserve", "USD/2", "250") // 25%
			expectBalance(ledgerName, "pt:main", "USD/2", "250")    // remaining
			expectBalance(ledgerName, "pt:src", "USD/2", "0")
		})
	})

	Context("colored balances (experimental-asset-colors)", Ordered, func() {
		const ledgerName = "nsx-colors"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should route funds by color if the feature is available (skip cleanly otherwise)", func() {
			// Mint colored funds from world into a pool, then spend a specific
			// color. Ledger volumes are keyed by (account, asset) only, so the
			// resulting posting is a normal-asset posting; we assert the color
			// send succeeds and the plain-asset balances settle.
			script := `
#![feature("experimental-asset-colors")]

send [COIN 100] (
  source = @world \ "RED"
  destination = @clr:pool
)

send [COIN 40] (
  source = @clr:pool \ "RED"
  destination = @clr:spent
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			if err != nil {
				if info := actions.ExtractGRPCErrorInfo(err); info != nil &&
					(info.Reason == domain.ErrReasonNumscriptParseError || info.Reason == domain.ErrReasonNumscriptRuntime) {
					Skip("experimental-asset-colors not available on this build: " + info.Reason)
				}
				Expect(err).To(Succeed())
			}

			expectBalance(ledgerName, "clr:pool", "COIN", "60")
			expectBalance(ledgerName, "clr:spent", "COIN", "40")
		})
	})

	// Stale-inputs retry: the FSM re-resolves dependencies against the
	// coverage-gated cache and returns retryable ErrStaleInputsResolution
	// (gRPC Unavailable) if a read value changed between admission and apply.
	// On a single-node testserver, admission and apply run back-to-back in one
	// Raft group, so the stale WINDOW is not deterministically reproducible at
	// e2e level (this is exercised deterministically at the processor unit
	// level via the InputsResolutionHash compare in
	// processor_transaction_numscript.go). Here we drive heavy concurrent
	// balance-reading transactions against a shared source and assert the
	// invariant the mechanism protects: every observed outcome is consistent —
	// a request either commits with the correct running balance or fails with
	// insufficient funds / the retryable stale error, and the final balance
	// exactly reflects the committed sends. No flakiness is forced.
	Context("concurrent balance-driven sends stay consistent", Ordered, func() {
		const ledgerName = "nsx-stale"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should never over-spend a shared balance-checked source under concurrency", func() {
			// Fund the source with exactly enough for N of the M concurrent
			// senders (each sends 100 from a source with 500). The excess must
			// be rejected (insufficient funds) or retried (stale) — never
			// silently over-committed.
			const funded = 500
			const perSend = 100
			const senders = 12
			fund(ledgerName, "stale:src", fmt.Sprintf("USD/2 %d", funded))

			script := `
send [USD/2 100] (
  source = @stale:src
  destination = @stale:dst
)
`
			var (
				wg        sync.WaitGroup
				mu        sync.Mutex
				succeeded int
			)
			for i := 0; i < senders; i++ {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
						actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
					if err == nil {
						mu.Lock()
						succeeded++
						mu.Unlock()
						return
					}
					// Only two failure modes are acceptable: insufficient funds
					// (balance ran out) or the retryable stale-inputs error.
					st, ok := status.FromError(err)
					Expect(ok).To(BeTrue(), "error must be a gRPC status: %v", err)
					info := actions.ExtractGRPCErrorInfo(err)
					Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
					Expect(info.Reason).To(BeElementOf(
						domain.ErrReasonInsufficientFunds,
						domain.ErrReasonStaleInputsResolution,
					), "unexpected failure reason %q (code=%s): %v", info.Reason, st.Code(), err)
				}()
			}
			wg.Wait()

			// The number of successful sends must not exceed the funded capacity,
			// and the resulting balances must match exactly.
			Expect(succeeded).To(BeNumerically("<=", funded/perSend))
			Expect(succeeded).To(BeNumerically(">", 0))

			expectBalance(ledgerName, "stale:src", "USD/2", fmt.Sprintf("%d", funded-succeeded*perSend))
			expectBalance(ledgerName, "stale:dst", "USD/2", fmt.Sprintf("%d", succeeded*perSend))
		})
	})
})
