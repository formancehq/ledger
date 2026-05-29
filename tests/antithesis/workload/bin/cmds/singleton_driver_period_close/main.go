package main

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
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

	// Period lifecycle loop:
	// 1. Close the current period (triggers checkpoint)
	// 2. Find CLOSED periods (sealed by the leader automatically)
	// 3. Archive them (uploads to cold storage / S3)
	// 4. Confirm the archive
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		closePeriod(ctx, client)
		archiveClosedPeriods(ctx, client)
	}
}

func closePeriod(ctx context.Context, client servicepb.BucketServiceClient) {
	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_ClosePeriod{
				ClosePeriod: &servicepb.ClosePeriodRequest{},
			},
		}},
	})

	if err != nil {
		if internal.IsTransient(err) {
			log.Printf("period close unavailable: %s", err)
			return
		}

		assert.Unreachable("period close returned unexpected error", internal.Details{"error": err})

		return
	}

	assert.Reachable("period close succeeded", nil)
	log.Println("period closed successfully")
}

func archiveClosedPeriods(ctx context.Context, client servicepb.BucketServiceClient) {
	periods, err := listPeriods(ctx, client)
	if err != nil {
		return
	}

	for _, p := range periods {
		if p.GetStatus() != commonpb.PeriodStatus_PERIOD_CLOSED {
			continue
		}

		periodID := p.GetId()
		details := internal.Details{"periodId": periodID}

		// Archive the closed period (uploads logs to cold storage).
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_ArchivePeriod{
					ArchivePeriod: &servicepb.ArchivePeriodRequest{
						PeriodId: periodID,
					},
				},
			}},
		})

		if err != nil {
			if internal.IsTransient(err) {
				continue
			}

			log.Printf("archive period %d failed: %s", periodID, err)

			continue
		}

		// Confirm the archive (purges hot data).
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_ConfirmArchivePeriod{
					ConfirmArchivePeriod: &servicepb.ConfirmArchivePeriodRequest{
						PeriodId: periodID,
					},
				},
			}},
		})

		if err != nil {
			if internal.IsTransient(err) {
				continue
			}

			log.Printf("confirm archive period %d failed: %s", periodID, err)

			continue
		}

		assert.Reachable("period archive completed", details)
		log.Printf("period %d archived and confirmed", periodID)
	}
}

func listPeriods(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Period, error) {
	stream, err := client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{})
	if err != nil {
		return nil, err
	}

	var periods []*commonpb.Period

	for {
		p, err := stream.Recv()
		if err == io.EOF {
			return periods, nil
		}
		if err != nil {
			return periods, err
		}

		periods = append(periods, p)
	}
}
