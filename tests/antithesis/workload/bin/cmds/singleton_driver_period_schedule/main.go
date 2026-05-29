package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("singleton_driver_period_schedule", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		details := internal.Details{}

		// 1. Set an automatic period schedule (every hour).
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SetPeriodSchedule{
					SetPeriodSchedule: &servicepb.SetPeriodScheduleRequest{
						Cron: "0 * * * *",
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to set period schedule", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// 2. Read the schedule back.
		schedResp, err := client.GetPeriodSchedule(ctx, &servicepb.GetPeriodScheduleRequest{})
		if err != nil {
			return
		}

		assert.AlwaysOrUnreachable(schedResp.GetCron() == "0 * * * *",
			"period schedule should match what was set",
			details.With(internal.Details{"actual": schedResp.GetCron()}))

		// 3. Delete the schedule.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_DeletePeriodSchedule{
					DeletePeriodSchedule: &servicepb.DeletePeriodScheduleRequest{},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to delete period schedule", details.With(internal.Details{"error": err}))
	})
}
