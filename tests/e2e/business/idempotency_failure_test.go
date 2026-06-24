//go:build e2e

package business

import (
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Regression for the ambiguous-commit / exactly-once gap: idempotency must
// return the SAME outcome for a key — including a definitive business failure —
// so a retry of a failed request replays that failure rather than re-executing.
//
// Without the fix, the first attempt fails a business check, the key is never
// recorded, and a retry (after the state changed so the same request would now
// succeed) re-executes and commits — a different outcome for the same key, the
// exact way the model driver loses track of committed/failed operations across
// a node restart.
var _ = Describe("Idempotency replays failures", Ordered, func() {
	const (
		ledgerName = "idempotency-failure-replay"
		idemKey    = "replay-failed-key"
	)

	// A transaction whose destination matches no declared account type — rejected
	// with ACCOUNT_NOT_MATCHING_TYPE while the chart has no "bank" type, accepted
	// once one is added. Identical bytes on every call so the idempotency hashes match.
	failingTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(
			idemKey,
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bank:1", big.NewInt(100), "USD"),
			}, nil, nil),
		)
	}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		// One unrelated type makes the chart enforced (strict by default), so an
		// account matching no type is rejected.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.AddAccountTypeAction(ledgerName, "wallet", "wallet:{id}")))
		Expect(err).To(Succeed())
	})

	It("replays the original failure for a retried key, even after state would make it succeed", func() {
		// 1. First attempt fails a definitive business check.
		_, err := sharedClient.Apply(sharedCtx, failingTx())
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
		firstMessage := status.Convert(err).Message()

		// 2. Change state so the identical request would now succeed.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.AddAccountTypeAction(ledgerName, "bank", "bank:{id}")))
		Expect(err).To(Succeed())

		// 3. Retry with the SAME idempotency key + identical payload. Exactly-once:
		//    the original failure is replayed, the request is NOT re-executed.
		_, err = sharedClient.Apply(sharedCtx, failingTx())
		Expect(err).To(HaveOccurred(),
			"retry of a failed idempotency key must replay the failure, not re-execute and commit")
		Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
		Expect(status.Convert(err).Message()).To(Equal(firstMessage))
	})

	It("never commits the retried transaction", func() {
		// bank:1 must stay untouched: the only writes for it were the replayed
		// failures. On the buggy path the retry committed 100 USD here.
		Consistently(func(g Gomega) {
			acct, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bank:1",
			})
			if status.Code(err) == codes.NotFound {
				return // never written — the expected outcome
			}
			g.Expect(err).To(Succeed())
			g.Expect(acct.GetVolumes()).To(BeEmpty())
		}).Within(2 * time.Second).ProbeEvery(250 * time.Millisecond).Should(Succeed())
	})
})

// Regression: capturing a failure outcome must never overwrite an existing
// idempotency entry. A conflict (same key, different payload) reaches the FSM
// and, on the buggy path, replaced the stored success with the conflict failure
// + new hash — after which the original request no longer replayed its committed
// result and instead conflicted.
var _ = Describe("Idempotency preserves committed outcomes", Ordered, func() {
	const (
		ledgerName = "idempotency-preserve-outcome"
		idemKey    = "committed-success-key"
	)

	okTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(
			idemKey,
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "wallet:1", big.NewInt(50), "USD"),
			}, nil, nil),
		)
	}

	// Same key, different amount — hash mismatch, so the FSM returns
	// ErrIdempotencyKeyConflict (AlreadyExists).
	conflictTx := actions.WithIdempotencyKey(
		idemKey,
		actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "wallet:1", big.NewInt(999), "USD"),
		}, nil, nil),
	)

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.AddAccountTypeAction(ledgerName, "wallet", "wallet:{id}")))
		Expect(err).To(Succeed())
	})

	It("does not let a conflicting retry overwrite a committed success", func() {
		// 1. First attempt commits.
		_, err := sharedClient.Apply(sharedCtx, okTx())
		Expect(err).To(Succeed())

		// 2. Same key, different payload — conflict. Must NOT overwrite the stored
		//    success.
		_, err = sharedClient.Apply(sharedCtx, conflictTx)
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.AlreadyExists))

		// 3. Retry the ORIGINAL payload+key — replays the committed success rather
		//    than conflicting against an overwritten failure entry.
		_, err = sharedClient.Apply(sharedCtx, okTx())
		Expect(err).To(Succeed(),
			"a conflicting retry must not overwrite the committed outcome of an idempotency key")
	})

	It("commits the original transaction exactly once", func() {
		// Only the first apply committed; the conflict rolled back and the replay
		// returned the original log without re-executing, so wallet:1 holds 50.
		acct, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: "wallet:1",
		})
		Expect(err).To(Succeed())
		usdVol, ok := acct.GetVolumes()["USD"]
		Expect(ok).To(BeTrue())
		Expect(usdVol.GetInput()).To(Equal("50"))
	})
})
