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

		It("Should allow a colored send from an UNbounded source (no balance read)", func() {
			// world is unbounded, so a colored world source never reads a balance —
			// nothing to collapse, nothing to double-spend. The destination volume
			// is a plain (account, asset) volume. This is the only colored-source
			// shape Ledger can serve soundly.
			script := `
#![feature("experimental-asset-colors")]

send [COIN 100] (
  source = @world \ "RED"
  destination = @clr:pool
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			if err != nil {
				if info := actions.ExtractGRPCErrorInfo(err); info != nil &&
					info.Reason == domain.ErrReasonNumscriptParseError {
					Skip("experimental-asset-colors not available on this build: " + info.Reason)
				}
				Expect(err).To(Succeed())
			}

			expectBalance(ledgerName, "clr:pool", "COIN", "100")
		})

		It("Should REJECT a colored send from a balance-checked source (P1-2)", func() {
			// Spending a specific color from a funded account reads that color's
			// balance. Ledger volumes have no color dimension, so this collapses to
			// the single COIN volume; serving the colored view would let the script
			// overspend. It must be rejected rather than silently double-counting.
			script := `
#![feature("experimental-asset-colors")]

send [COIN 40] (
  source = @clr:pool \ "RED"
  destination = @clr:spent
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			if err != nil {
				if info := actions.ExtractGRPCErrorInfo(err); info != nil &&
					info.Reason == domain.ErrReasonNumscriptParseError {
					Skip("experimental-asset-colors not available on this build: " + info.Reason)
				}
			}
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonValidation),
				"colored balance read must be rejected as validation, got %q", info.Reason)

			// pool untouched.
			expectBalance(ledgerName, "clr:pool", "COIN", "100")
		})
	})

	// Experimental var-origin functions that route through the new
	// ResolveDependencies → preload → hash → FSM path: overdraft() (the
	// function, distinct from `allowing overdraft`), get_asset (can decide the
	// write asset/volume), get_amount, and mid-script function calls (balance
	// reads outside the vars block). Each declares its feature in-script; the
	// FSM merges #![feature] into the run flag set, and resolution enables every
	// feature (nil flag set), so the discovered deps must be preloaded for apply
	// to resolve deterministically.
	Context("experimental var-origin functions (get_amount / get_asset / overdraft() / mid-script)", Ordered, func() {
		const ledgerName = "nsx-fns"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("overdraft() function: reads the account's overdraft and sizes a send", func() {
			// Drive an account negative so overdraft() returns a non-zero amount,
			// then send exactly that amount out of world. overdraft(@fns:od, USD/2)
			// reads @fns:od's balance — that read must be discovered/preloaded, and
			// its value is a bound input.
			//
			// Put @fns:od at -200 first (unbounded overdraft), so
			// overdraft(@fns:od, USD/2) == 200.
			script := `
#![feature("experimental-overdraft-function")]

vars {
  monetary $od = overdraft(@fns:od, USD/2)
}

send $od (
  source = @world
  destination = @fns:od_out
)
`
			// Establish the negative balance.
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, `
send [USD/2 200] (
  source = @fns:od allowing unbounded overdraft
  destination = @fns:sink
)
`, nil, nil)))
			Expect(err).To(Succeed())
			expectBalance(ledgerName, "fns:od", "USD/2", "-200")

			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			// overdraft was 200, so 200 is sent world -> od_out.
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("200"))
			expectBalance(ledgerName, "fns:od_out", "USD/2", "200")
		})

		It("get_amount(): derives a send amount from a read balance", func() {
			fund(ledgerName, "fns:ga_src", "USD/2 640")

			script := `
#![feature("experimental-get-amount-function")]

vars {
  monetary $m = balance(@fns:ga_src, USD/2)
  number $n = get_amount($m)
}

send [USD/2 $n] (
  source = @fns:ga_src
  destination = @fns:ga_dst
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("640"))

			expectBalance(ledgerName, "fns:ga_src", "USD/2", "0")
			expectBalance(ledgerName, "fns:ga_dst", "USD/2", "640")
		})

		It("get_asset(): the asset of a read balance decides the write volume's asset", func() {
			fund(ledgerName, "fns:as_src", "USD/2 300")

			// $a is the asset extracted from a balance() monetary; the send uses it,
			// so the destination volume is keyed by that asset. The backing
			// balance() read (as_src/USD/2) must be discovered and preloaded.
			script := `
#![feature("experimental-get-asset-function")]

vars {
  monetary $m = balance(@fns:as_src, USD/2)
  asset $a = get_asset($m)
}

send [$a 120] (
  source = @fns:as_src
  destination = @fns:as_dst
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Asset).To(Equal("USD/2"))

			expectBalance(ledgerName, "fns:as_src", "USD/2", "180")
			expectBalance(ledgerName, "fns:as_dst", "USD/2", "120")
		})

		It("mid-script function call: a balance() outside vars is discovered and stored", func() {
			// balance() called inline in set_tx_meta (not in the vars block) reads
			// @fns:ms_probe's balance. That read must be discovered/preloaded, and
			// the stored metadata reflects the real balance.
			fund(ledgerName, "fns:ms_probe", "USD/2 512")

			script := `
#![feature("experimental-mid-script-function-call")]

send [USD/2 10] (
  source = @world
  destination = @fns:ms_dst
)

set_tx_meta("probe_balance", balance(@fns:ms_probe, USD/2))
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			meta := commonpb.MetadataToGoMap(createdTx.Transaction.Metadata)
			// balance() of 512 stored as a monetary => "USD/2 512".
			Expect(meta["probe_balance"]).To(Equal("USD/2 512"))

			expectBalance(ledgerName, "fns:ms_dst", "USD/2", "10")
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

	// EN-1406 P1-1: within a single atomic batch the FSM applies orders
	// sequentially against a mutated WriteSet, so a later order whose
	// balance()/meta() depends on an earlier order in the same batch must resolve
	// against the earlier order's effect. Admission now layers preceding orders'
	// effects into resolution; without the fix the dependent order hashed stale
	// and was rejected as STALE_INPUTS_RESOLUTION on every retry (permanent fail).
	Context("intra-batch dependent resolution (P1-1)", Ordered, func() {
		const ledgerName = "nsx-intrabatch"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should resolve and commit an order that reads a balance an earlier batch order set", func() {
			// Order 1 deposits 100 into bulk:source; order 2 sends
			// balance(@bulk:source) onward. Order 2's resolution must see order 1's
			// deposit (balance 100), send exactly 100, and both commit.
			depositScript := `send [USD/2 100] (source = @world destination = @bulk:source)`
			forwardScript := `
vars {
  monetary $all = balance(@bulk:source, USD/2)
}
send $all (
  source = @bulk:source
  destination = @bulk:dest
)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, depositScript, nil, nil),
				actions.CreateScriptTransactionAction(ledgerName, forwardScript, nil, nil),
			))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			forwardTx := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(forwardTx.Transaction.Postings).To(HaveLen(1))
			Expect(forwardTx.Transaction.Postings[0].Source).To(Equal("bulk:source"))
			Expect(forwardTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("100"))

			expectBalance(ledgerName, "bulk:source", "USD/2", "0")
			expectBalance(ledgerName, "bulk:dest", "USD/2", "100")
		})

		It("Should resolve meta() a preceding batch order wrote", func() {
			// Order 1 sets routing metadata (a script needs at least one send, so it
			// also moves a token amount); order 2 reads the metadata via meta() to
			// pick the destination. Order 2 must see order 1's write within the same
			// batch.
			setMetaScript := `
set_account_meta(@ib:cfg, "dest", "ib:resolved")
send [USD/2 1] (source = @world destination = @ib:cfg)
`
			useMetaScript := `
vars {
  account $dst = meta(@ib:cfg, "dest")
}
send [USD/2 25] (source = @world destination = $dst)
`
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, setMetaScript, nil, nil),
				actions.CreateScriptTransactionAction(ledgerName, useMetaScript, nil, nil),
			))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			useTx := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(useTx.Transaction.Postings).To(HaveLen(1))
			Expect(useTx.Transaction.Postings[0].Destination).To(Equal("ib:resolved"))

			expectBalance(ledgerName, "ib:resolved", "USD/2", "25")
		})
	})

	// EN-1406 P1-2: Ledger volumes carry no color/scope dimension, so a script
	// that reads a color-qualified balance must be rejected — serving each color
	// view the full balance would let one script spend the same funds once per
	// color and drive the volume negative with no overdraft clause (double-spend).
	Context("colored balance reads are rejected (P1-2)", Ordered, func() {
		const ledgerName = "nsx-color-reject"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should reject a script that spends the same volume across two colors", func() {
			// Fund the source with a plain COIN 100. The script tries to spend 80
			// RED + 80 BLUE from it. Both colored views collapse to the one COIN
			// volume, so allowing this would overspend to -60. Must be rejected.
			fund(ledgerName, "clr:src", "COIN 100")

			script := `
#![feature("experimental-asset-colors")]

send [COIN 80] (
  source = @clr:src \ "RED"
  destination = @clr:out
)

send [COIN 80] (
  source = @clr:src \ "BLUE"
  destination = @clr:out
)
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonValidation),
				"colored balance reads must be rejected as a validation error, got %q", info.Reason)

			// The source must be untouched — nothing committed.
			expectBalance(ledgerName, "clr:src", "COIN", "100")
		})
	})

	// EN-1406 P1-3: InputsResolutionHash must NOT participate in the idempotency
	// hash. A retry of the SAME keyed request that reads state (balance) would
	// otherwise re-resolve at a changed balance, hash differently, and be
	// rejected as IDEMPOTENCY_KEY_CONFLICT instead of replaying the first log.
	Context("idempotent replay of a state-reading script (P1-3)", Ordered, func() {
		const ledgerName = "nsx-idem"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should replay (not conflict) when the same keyed request re-resolves at a changed balance", func() {
			// Seed the source with 100. The keyed script reads balance and sends a
			// fixed 10. The FIRST apply commits (balance 100 → 90). A second apply
			// with the SAME idempotency key re-resolves at balance 90 — a different
			// InputsResolutionHash — but must REPLAY the first log, not conflict.
			fund(ledgerName, "idem:src", "USD/2 100")

			const idemKey = "idem-key-p1-3"
			script := `
vars {
  monetary $bal = balance(@idem:src, USD/2)
}
send [USD/2 10] (
  source = @idem:src
  destination = @idem:dst
)
`
			resp1, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest(idemKey,
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp1.Logs).To(HaveLen(1))
			txID1 := resp1.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Balance is now 90; a naive re-resolution hashes differently.
			expectBalance(ledgerName, "idem:src", "USD/2", "90")

			// Retry under the same key: must replay the original log (same tx id),
			// NOT conflict and NOT double-spend.
			resp2, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest(idemKey,
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed(), "identical keyed retry must replay, not conflict")
			Expect(resp2.Logs).To(HaveLen(1))
			txID2 := resp2.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(txID2).To(Equal(txID1), "retry must replay the same transaction")

			// Balance unchanged by the replay — no second debit.
			expectBalance(ledgerName, "idem:src", "USD/2", "90")
			expectBalance(ledgerName, "idem:dst", "USD/2", "10")
		})
	})
})
