// parallel_driver_stats_consistency verifies that GetLedgerStats returns
// self-consistent values and that AggregateVolumes sums to zero (double-entry).
//
// These invariants hold regardless of concurrent writes:
//   - Main-store structural: logCount >= txCount.
//   - Double-entry: sum(input) == sum(output) for each asset (AggregateVolumes
//     reads from the sequentially-applied read-index, so a transaction is either
//     fully included or not — partial postings are impossible).
//
// Usage counters are asynchronous and may be ahead of or behind the main
// snapshot. eventually_stats_consistency checks them against complete logs at
// a fixed source horizon after an independent usage witness has been observed.
package main

import (
	"context"
	"log"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_stats_consistency", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		details := internal.Details{"ledger": ledger}

		stats, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: ledger})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			log.Printf("stats: GetLedgerStats failed: %s", err)

			return
		}

		statsDetails := details.With(internal.Details{
			"transactionCount": stats.GetTransactionCount(),
			"volumeCount":      stats.GetVolumeCount(),
			"postingCount":     stats.GetPostingCount(),
			"revertCount":      stats.GetRevertCount(),
			"logCount":         stats.GetLogCount(),
		})

		assert.Always(stats.GetTransactionCount() >= 0, "transaction count must be non-negative", statsDetails)
		assert.Always(mainStoreStatsConsistent(stats),
			"log count must be >= transaction count (logs include metadata, reverts, etc.)", statsDetails)

		// Aggregate volumes must sum to zero (double-entry invariant).
		aggResp, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
			Ledger: ledger,
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			log.Printf("stats: AggregateVolumes failed: %s", err)

			return
		}

		for _, vol := range aggResp.GetVolumes() {
			input := vol.GetInput().ToBigInt()
			output := vol.GetOutput().ToBigInt()
			balance := new(big.Int).Sub(input, output)

			assert.Always(
				balance.Sign() == 0,
				"aggregate balance must be zero (double-entry)",
				statsDetails.With(internal.Details{
					"asset":   vol.GetAsset(),
					"input":   input.String(),
					"output":  output.String(),
					"balance": balance.String(),
				}),
			)
		}

		assert.Reachable("stats consistency check passed", statsDetails)
		log.Printf("stats: check passed for %s (txs=%d, vols=%d, logs=%d)",
			ledger, stats.GetTransactionCount(), stats.GetVolumeCount(), stats.GetLogCount())
	})
}

func mainStoreStatsConsistent(stats *commonpb.LedgerStats) bool {
	return stats.GetLogCount() >= stats.GetTransactionCount()
}
