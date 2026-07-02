// Driver for the "stale-reads-are-prefix-consistent" property: a read issued
// with "x-consistency: stale" metadata (consistency.go:23, routed to the local
// store with NO sync-status check, controller_routed.go:67-70) may lag, but it
// must still serve SOME prefix of the applied log. On a probe account that
// only ever receives single-unit increments, every prefix satisfies
// 0 ≤ balance ≤ writes-attempted-so-far, input == balance and output == 0.
//
// Soundness against benign interleavings (faults, retries, round-robin):
//   - Owned "stale-" ledger + dedicated probe account: no foreign writes, so
//     the attempted/acked counters are exact.
//   - Unique reference per increment: a transparent UNAVAILABLE retry that
//     re-proposes an already-committed increment is rejected AlreadyExists —
//     each logical increment commits at most once, making `attempted` a sound
//     upper bound even when an errored Apply secretly committed. AlreadyExists
//     on our own unique reference proves the increment IS committed → acked.
//   - The upper bound is `attempted`, not `acked`: attempted ≥ acked, and an
//     ambiguous (errored) attempt may be visible to a later read.
//   - The connection round-robins across nodes, so the serving node of any
//     given read is NOT attributable. Per-node session monotonicity (the
//     evidence's second Always) is therefore DEFERRED to the per-node dial
//     helper (Lot F); only the node-agnostic bounds are asserted here.
//   - Transient read errors (Unavailable, NotFound from a node lagging behind
//     the ledger creation) → that read is skipped, never asserted.
package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const (
	probeAccount = "probe:counter"
	probeAsset   = "USD/2"
)

// parseAmount parses a big.Int volume string, defaulting to 0 for empty
// values (an absent volume entry is a valid prefix: no write applied yet).
func parseAmount(s string) *big.Int {
	if s == "" {
		return big.NewInt(0)
	}

	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil
	}

	return v
}

func main() {
	internal.RunDriver("parallel_driver_stale_reads", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := internal.PrefixStaleReads.WithSeed(run)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// The header key/value mirror internal/adapter/grpc/consistency.go
		// (metadataKeyConsistency / ConsistencyStale).
		staleCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "stale")

		// Menu axis: number of increment+read rounds.
		rounds := antirandom.RandomChoice([]int{5, 15})

		var (
			attempted   int
			acked       int
			lagObserved bool
			anyStale    bool
		)

		for i := range rounds {
			attempted++

			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: probeAccount,
									Amount:      commonpb.NewUint256FromUint64(1),
									Asset:       probeAsset,
								}},
								Reference: fmt.Sprintf("stale-%d-%d", run, i),
								Force:     true,
							},
						}},
					},
				},
			}))

			switch {
			case err == nil:
				acked++
			case internal.IsAlreadyExists(err):
				// Our reference is unique: AlreadyExists means an earlier
				// (retried) attempt of THIS increment committed.
				acked++
			default:
				// Ambiguous: may or may not have committed; attempted already
				// counts it, keeping the upper bound sound.
			}

			account, err := client.GetAccount(staleCtx, &servicepb.GetAccountRequest{
				Ledger:  ledger,
				Address: probeAccount,
			})
			if err != nil {
				// Transient, ctx canceled (driver shutting down), or the
				// serving node's prefix predates the ledger (NotFound is
				// itself a valid prefix) — skip this read.
				if !internal.IsTolerated(err) && !internal.IsNotFound(err) {
					assert.Unreachable("stale read GetAccount returned unexpected error",
						internal.Details{"ledger": ledger, "round": i, "error": err})
				}

				continue
			}

			anyStale = true

			details := internal.Details{
				"ledger":    ledger,
				"round":     i,
				"attempted": attempted,
				"acked":     acked,
			}

			vol := account.GetVolumes()[probeAsset]
			if vol == nil {
				// Valid prefix: account materialized, no write applied yet.
				continue
			}

			var (
				input   = parseAmount(vol.GetInput())
				output  = parseAmount(vol.GetOutput())
				balance = parseAmount(vol.GetBalance())
			)

			if input == nil || output == nil || balance == nil {
				assert.Unreachable("stale read returned unparsable volume strings",
					details.With(internal.Details{
						"input":   vol.GetInput(),
						"output":  vol.GetOutput(),
						"balance": vol.GetBalance(),
					}))

				continue
			}

			volDetails := details.With(internal.Details{
				"input":   input.String(),
				"output":  output.String(),
				"balance": balance.String(),
			})

			assert.Always(balance.Sign() >= 0 && balance.Cmp(big.NewInt(int64(attempted))) <= 0,
				"stale read balance stays within attempted write bounds",
				volDetails)

			// The probe only ever receives: any served prefix has
			// input == balance and output == 0; anything else is a torn read.
			assert.Always(input.Cmp(balance) == 0 && output.Sign() == 0,
				"stale read probe volumes are single-direction consistent",
				volDetails)

			if balance.Cmp(big.NewInt(int64(acked))) < 0 {
				lagObserved = true
			}
		}

		summary := internal.Details{"ledger": ledger, "attempted": attempted, "acked": acked}

		assert.Sometimes(anyStale, "stale read of probe account succeeded", summary)
		// Meaningful staleness was actually observed at least once across the
		// run — otherwise the bounds above only ever saw fresh state.
		assert.Sometimes(lagObserved, "stale read observed lag behind acked writes", summary)
	})
}
