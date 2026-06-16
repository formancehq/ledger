//go:build e2e

package business

import (
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// EphemeralPurgeRace exercises the admission/apply race where two concurrent
// transactions target the same ephemeral account.
//
// Scenario: a wallets:{id} account holds a non-zero balance and is in the
// leader's cache. Two transactions arrive close together:
//   - A drains the account to zero (triggers ephemeral purge → cache eviction)
//   - B touches the same account (must read its volume at apply time)
//
// Both are admitted while the cache holds the account, so CheckCache returns
// CacheGuaranteed and neither admission emits a preload for it. If they
// co-batch on apply with A first, A's applyEphemeralPurge calls
// fsm.Registry.Volumes.Delete which clears the in-memory cache immediately.
// B then runs in the same batch with no preload and an empty cache, and
// applyPosting fails with "source volume … not preloaded".
var _ = Describe("EphemeralPurgeRace", Ordered, func() {
	const (
		ledgerName = "ephemeral-purge-race"
		iterations = 30
	)

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateLedgerAction(ledgerName, nil),
				actions.AddEphemeralAccountTypeAction(ledgerName, "wallets", "wallets:{id}"),
			),
		})
		Expect(err).To(Succeed())
	})

	It("Should not fail with 'not preloaded' when concurrent drains race with purge", func() {
		var raceHits []string

		for i := 0; i < iterations; i++ {
			account := fmt.Sprintf("wallets:race-%d", i)

			// Seed the ephemeral account with a non-zero balance so the leader's
			// cache holds it at the moment admission inspects CheckCache.
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", account, big.NewInt(100), "USD"),
					}, nil),
				),
			})
			Expect(err).To(Succeed())

			// Issue A and B through a barrier so they reach admission together
			// and (hopefully) co-batch on apply.
			barrier := make(chan struct{})
			var (
				wg         sync.WaitGroup
				errA, errB error
			)
			wg.Add(2)

			go func() {
				defer wg.Done()
				<-barrier
				_, errA = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Envelopes: servicepb.UnsignedEnvelopes(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting(account, "world", big.NewInt(100), "USD"),
						}, nil, nil),
					),
				})
			}()

			go func() {
				defer wg.Done()
				<-barrier
				_, errB = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Envelopes: servicepb.UnsignedEnvelopes(
						actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting(account, "world", big.NewInt(50), "USD"),
						}, nil),
					),
				})
			}()

			close(barrier)
			wg.Wait()

			// The race signature is the "not preloaded" string in either error.
			if errA != nil && strings.Contains(errA.Error(), "not preloaded") {
				raceHits = append(raceHits, fmt.Sprintf("iter %d A: %v", i, errA))
			}
			if errB != nil && strings.Contains(errB.Error(), "not preloaded") {
				raceHits = append(raceHits, fmt.Sprintf("iter %d B: %v", i, errB))
			}
		}

		Expect(raceHits).To(BeEmpty(),
			"ephemeral-purge race fired in %d/%d iterations:\n%s",
			len(raceHits), iterations, strings.Join(raceHits, "\n"))
	})
})
