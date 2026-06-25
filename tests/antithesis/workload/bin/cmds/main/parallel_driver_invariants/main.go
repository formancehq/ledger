package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_invariants", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		resp, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
			Ledger: ledger,
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			assert.Unreachable("AggregateVolumes returned unexpected error", internal.Details{
				"ledger": ledger,
				"error":  err,
			})

			return
		}

		for _, vol := range resp.GetVolumes() {
			input := vol.GetInput().ToBigInt()
			output := vol.GetOutput().ToBigInt()

			assert.Always(input.Cmp(output) == 0, "aggregate double-entry: input must equal output", internal.Details{
				"ledger": ledger,
				"asset":  vol.GetAsset(),
				"input":  input.String(),
				"output": output.String(),
			})
		}
	})
}
