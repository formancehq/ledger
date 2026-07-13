//go:build e2e

package business

import (
	"fmt"
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

// NumscriptLanguage exercises Numscript language surface that the other suites
// (numscript_test.go, numscript_experimental_test.go) do not yet cover
// end-to-end: send-all, the save statement, capped / allotment sources, inorder
// destinations with caps and kept, every externally-typed variable, value
// expressions (infix + - /, prefix minus), and the runtime error paths those
// features can raise. Each spec submits a real program through the normal
// create-transaction path and asserts the committed postings and balances (or
// the exact rejection reason), so it validates numscript semantics as the ledger
// actually executes them, not just that the request succeeds.
var _ = Describe("NumscriptLanguage", Ordered, func() {

	// getVolume reads back an account balance for an asset, retrying to absorb
	// read-side projection lag.
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

	apply := func(ledgerName, script string, vars map[string]string) (*servicepb.ApplyResponse, error) {
		return sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateScriptTransactionAction(ledgerName, script, vars, nil)))
	}

	Context("send-all ([ASSET *])", Ordered, func() {
		const ledgerName = "nsl-sendall"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should drain the entire available balance of a bounded source", func() {
			// send-all pulls the source's whole balance. The exact amount is
			// therefore balance-dependent and must be discovered/preloaded.
			fund(ledgerName, "sa:src", "USD/2 725")

			script := `
send [USD/2 *] (
  source = @sa:src
  destination = @sa:dst
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("725"))

			expectBalance(ledgerName, "sa:src", "USD/2", "0")
			expectBalance(ledgerName, "sa:dst", "USD/2", "725")
		})

		It("Should reject send-all from an empty source (no posting produced)", func() {
			// An empty source has nothing to send; send-all yields zero postings.
			// A transaction must produce at least one posting, so the ledger
			// rejects the empty result as a validation error.
			script := `
send [USD/2 *] (
  source = @sa:empty
  destination = @sa:void
)
`
			_, err := apply(ledgerName, script, nil)
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonValidation),
				"a transaction producing no postings must be rejected as validation, got %q", info.Reason)
		})

		It("Should reject send-all from an unbounded @world source", func() {
			// send-all from @world is unbounded — there is no finite balance to
			// drain, so the interpreter raises a runtime error.
			script := `
send [USD/2 *] (
  source = @world
  destination = @sa:dst2
)
`
			_, err := apply(ledgerName, script, nil)
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptRuntime),
				"send-all from unbounded source must be a runtime error, got %q", info.Reason)
		})

		It("Should reject send-all from an unbounded-overdraft source", func() {
			script := `
send [USD/2 *] (
  source = @sa:credit allowing unbounded overdraft
  destination = @sa:dst3
)
`
			_, err := apply(ledgerName, script, nil)
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptRuntime),
				"send-all from unbounded-overdraft source must be a runtime error, got %q", info.Reason)
		})
	})

	Context("save statement", Ordered, func() {
		const ledgerName = "nsl-save"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should reserve funds with save so a later send sees less balance", func() {
			// save [USD/2 100] from @acct sets aside 100 of the account's balance
			// for the rest of the script: a subsequent send that would otherwise
			// succeed now fails because the reserved amount is unavailable.
			fund(ledgerName, "sv:acct", "USD/2 300")

			script := `
save [USD/2 100] from @sv:acct

send [USD/2 250] (
  source = @sv:acct
  destination = @sv:dst
)
`
			_, err := apply(ledgerName, script, nil)
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonInsufficientFunds),
				"save-reserved funds must be excluded from the send, got %q", info.Reason)

			// Nothing committed: balance is untouched.
			expectBalance(ledgerName, "sv:acct", "USD/2", "300")
		})

		It("Should allow a send that fits within the un-reserved remainder", func() {
			// With 300 funded and 100 saved, up to 200 remains spendable.
			fund(ledgerName, "sv:acct2", "USD/2 300")

			script := `
save [USD/2 100] from @sv:acct2

send [USD/2 200] (
  source = @sv:acct2
  destination = @sv:dst2
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("200"))

			// save produces no posting of its own; only the send moved funds.
			expectBalance(ledgerName, "sv:acct2", "USD/2", "100")
			expectBalance(ledgerName, "sv:dst2", "USD/2", "200")
		})

		It("Should reserve the whole balance with save [ASSET *]", func() {
			// save [USD/2 *] reserves the entire balance, so any positive send
			// afterwards fails for lack of available funds.
			fund(ledgerName, "sv:acct3", "USD/2 150")

			script := `
save [USD/2 *] from @sv:acct3

send [USD/2 1] (
  source = @sv:acct3
  destination = @sv:dst3
)
`
			_, err := apply(ledgerName, script, nil)
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonInsufficientFunds))

			expectBalance(ledgerName, "sv:acct3", "USD/2", "150")
		})
	})

	Context("capped source (max CAP from SOURCE)", Ordered, func() {
		const ledgerName = "nsl-capped"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should cap the pull from the first source and overflow to the fallback", func() {
			// The first source is capped at 100 even though it holds 1000; the
			// remaining 150 of a 250 send comes from world. Two postings result.
			fund(ledgerName, "cp:vault", "USD/2 1000")

			script := `
send [USD/2 250] (
  source = {
    max [USD/2 100] from @cp:vault
    @world
  }
  destination = @cp:out
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))

			// vault contributes exactly the cap (100); world the rest (150).
			expectBalance(ledgerName, "cp:vault", "USD/2", "900")
			expectBalance(ledgerName, "cp:out", "USD/2", "250")
		})
	})

	Context("allotment source (portioned sources)", Ordered, func() {
		const ledgerName = "nsl-alloc-src"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should split the requested amount across sources by portion", func() {
			// The send amount is split by portion across the sources: 1/4 from A,
			// remaining (3/4) from B. Both sources must hold enough.
			fund(ledgerName, "as:a", "USD/2 500")
			fund(ledgerName, "as:b", "USD/2 500")

			script := `
send [USD/2 400] (
  source = {
    1/4 from @as:a
    remaining from @as:b
  }
  destination = @as:sink
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))

			// 1/4 of 400 = 100 from A, 300 from B.
			expectBalance(ledgerName, "as:a", "USD/2", "400")
			expectBalance(ledgerName, "as:b", "USD/2", "200")
			expectBalance(ledgerName, "as:sink", "USD/2", "400")
		})
	})

	Context("inorder destination (caps, remaining, kept)", Ordered, func() {
		const ledgerName = "nsl-dest-inorder"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should fill destinations up to caps and route the remainder", func() {
			// Distribute 100 from world: up to 20 to alice, up to 30 to bob, the
			// remaining 50 to charlie.
			script := `
send [USD/2 100] (
  source = @world
  destination = {
    max [USD/2 20] to @di:alice
    max [USD/2 30] to @di:bob
    remaining to @di:charlie
  }
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(3))

			expectBalance(ledgerName, "di:alice", "USD/2", "20")
			expectBalance(ledgerName, "di:bob", "USD/2", "30")
			expectBalance(ledgerName, "di:charlie", "USD/2", "50")
		})

		It("Should keep the unrouted remainder at the source with remaining kept", func() {
			// 60 sent from a funded account: 40 goes to a payee, the remaining 20
			// is kept at the source (no posting for the kept part).
			fund(ledgerName, "di:payer", "USD/2 60")

			script := `
send [USD/2 60] (
  source = @di:payer
  destination = {
    max [USD/2 40] to @di:payee
    remaining kept
  }
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			// Only the 40 that actually moved produces a posting; the 20 kept does
			// not round-trip through the source.
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("40"))

			// payer keeps 20 (60 funded - 40 sent), payee received 40.
			expectBalance(ledgerName, "di:payer", "USD/2", "20")
			expectBalance(ledgerName, "di:payee", "USD/2", "40")
		})
	})

	Context("externally-typed variables", Ordered, func() {
		const ledgerName = "nsl-vars"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should bind account, asset, monetary, number, string and portion vars from the vars map", func() {
			// One program that consumes every externally-typed variable form the
			// client can pass through the string vars map:
			//   - account : resolved source AND destinations ($primary/$secondary)
			//   - asset   : the send asset
			//   - number  : interpolated amount via [ $asset $count ]
			//   - monetary: a whole literal amount ($bonus) sent in a second block
			//   - portion : a split ratio for the destination
			//   - string  : stored via set_tx_meta
			fund(ledgerName, "vr:src", "USD/2 1000")

			// Every declared var is consumed: the account vars drive the
			// destinations (not hard-coded account literals), and the monetary var
			// $bonus is sent whole in its own block so its typed form is exercised.
			script := `
vars {
  account $source
  account $primary
  account $secondary
  asset $currency
  number $count
  monetary $bonus
  portion $split
  string $note
}

set_tx_meta("note", $note)

send [$currency $count] (
  source = $source
  destination = {
    $split to $primary
    remaining to $secondary
  }
)

send $bonus (
  source = $source
  destination = $primary
)
`
			resp, err := apply(ledgerName, script, map[string]string{
				"source":    "vr:src",
				"primary":   "vr:primary",
				"secondary": "vr:secondary",
				"currency":  "USD/2",
				"count":     "400",
				"bonus":     "USD/2 50",
				"split":     "1/4",
				"note":      "typed-vars",
			})
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			// 3 postings: split 1/4 (=100) to primary, remaining (=300) to
			// secondary, plus the $bonus (=50) to primary.
			Expect(createdTx.Transaction.Postings).To(HaveLen(3))
			Expect(createdTx.Transaction.Postings[0].Asset).To(Equal("USD/2"))

			meta := commonpb.MetadataToGoMap(createdTx.Transaction.Metadata)
			Expect(meta["note"]).To(Equal("typed-vars"))

			// src sends 400 + 50 = 450 (1000 - 450 = 550 left).
			// primary receives 100 (split) + 50 (bonus) = 150; secondary 300.
			expectBalance(ledgerName, "vr:src", "USD/2", "550")
			expectBalance(ledgerName, "vr:primary", "USD/2", "150")
			expectBalance(ledgerName, "vr:secondary", "USD/2", "300")
		})

		It("Should accept a portion passed as a percentage string", func() {
			// ParsePortionSpecific accepts both fraction ("1/4") and percentage
			// ("30%") forms for a portion var.
			fund(ledgerName, "vr:src2", "USD/2 1000")

			script := `
vars {
  portion $split
}

send [USD/2 1000] (
  source = @vr:src2
  destination = {
    $split to @vr:cut
    remaining to @vr:rest
  }
)
`
			resp, err := apply(ledgerName, script, map[string]string{
				"split": "30%",
			})
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))

			expectBalance(ledgerName, "vr:cut", "USD/2", "300")
			expectBalance(ledgerName, "vr:rest", "USD/2", "700")
		})
	})

	Context("value expressions (infix and prefix operators)", Ordered, func() {
		const ledgerName = "nsl-expr"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should evaluate monetary addition in a send amount", func() {
			// [USD/2 100] + [USD/2 50] = [USD/2 150]. Same-asset monetary addition.
			script := `
vars {
  monetary $base = [USD/2 100]
}

send $base + [USD/2 50] (
  source = @world
  destination = @ex:add
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("150"))
			expectBalance(ledgerName, "ex:add", "USD/2", "150")
		})

		It("Should evaluate monetary subtraction in a send amount", func() {
			// [USD/2 200] - [USD/2 30] = [USD/2 170].
			script := `
send [USD/2 200] - [USD/2 30] (
  source = @world
  destination = @ex:sub
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("170"))
			expectBalance(ledgerName, "ex:sub", "USD/2", "170")
		})

		It("Should derive a portion via number division and split accordingly", func() {
			// 250 / 1000 evaluates to the portion 1/4 (number division yields a
			// portion). Using it as a destination allotment routes 1/4 of 1000 to
			// @cut. (Monetary/monetary division is a type error, so the operands
			// are plain numbers here.)
			fund(ledgerName, "ex:src", "USD/2 1000")

			script := `
vars {
  portion $p = 250 / 1000
}

send [USD/2 1000] (
  source = @ex:src
  destination = {
    $p to @ex:cut
    remaining to @ex:rest
  }
)
`
			resp, err := apply(ledgerName, script, nil)
			Expect(err).To(Succeed())
			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx.Transaction.Postings).To(HaveLen(2))

			expectBalance(ledgerName, "ex:cut", "USD/2", "250")
			expectBalance(ledgerName, "ex:rest", "USD/2", "750")
		})
	})

	Context("runtime error paths", Ordered, func() {
		const ledgerName = "nsl-errors"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should reject a division by zero as a runtime error", func() {
			script := `
set_tx_meta("bad", 3 / 0)

send [USD/2 1] (
  source = @world
  destination = @er:dst
)
`
			_, err := apply(ledgerName, script, nil)
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptRuntime),
				"division by zero must be a runtime error, got %q", info.Reason)
		})

		It("Should reject a send of a negative amount as a runtime error", func() {
			// A prefix-negated monetary produces a negative amount, which is
			// invalid in a send position.
			script := `
send -[USD/2 100] (
  source = @world
  destination = @er:neg
)
`
			_, err := apply(ledgerName, script, nil)
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptRuntime),
				"negative send amount must be a runtime error, got %q", info.Reason)
		})

		It("Should reject a non-numeric value for a number variable", func() {
			// A number var whose raw value cannot parse as an integer is a parse
			// error surfaced at variable binding.
			script := `
vars {
  number $count
}

send [USD/2 $count] (
  source = @world
  destination = @er:num
)
`
			_, err := apply(ledgerName, script, map[string]string{
				"count": "not-a-number",
			})
			Expect(err).To(HaveOccurred())
			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil(), "error must carry error info: %v", err)
			Expect(info.Reason).To(BeElementOf(
				domain.ErrReasonNumscriptParseError,
				domain.ErrReasonNumscriptRuntime,
				domain.ErrReasonValidation,
			), "invalid number var must be rejected, got %q", info.Reason)
		})
	})
})
