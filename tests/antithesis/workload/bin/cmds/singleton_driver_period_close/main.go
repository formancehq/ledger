package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: singleton_driver_period_close")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	// Close periods in a loop. Period closes trigger Pebble checkpoints,
	// which interact with the snapshot/restore mechanism under fault injection.
	// This exercises the maintenance task pipeline (gating, spool replay).
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_ClosePeriod{
					ClosePeriod: &servicepb.ClosePeriodRequest{},
				},
			}},
		})

		if err != nil {
			if internal.IsUnavailable(err) {
				log.Printf("period close unavailable, retrying: %s", err)
				continue
			}

			assert.Unreachable("period close returned unexpected error", internal.Details{
				"error": err,
			})

			continue
		}

		assert.Reachable("period close succeeded", nil)
		log.Println("period closed successfully")
	}
}
