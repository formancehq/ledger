package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("singleton_driver_chapter_schedule", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		details := internal.Details{}

		// 1. Set an automatic chapter schedule (every hour).
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SetChapterSchedule{
				SetChapterSchedule: &servicepb.SetChapterScheduleRequest{
					Cron: "0 * * * *",
				},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to set chapter schedule", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// 2. Read the schedule back.
		schedResp, err := client.GetChapterSchedule(ctx, &servicepb.GetChapterScheduleRequest{})
		if err != nil {
			internal.LogCleanupError("get chapter schedule after set", err)
			return
		}

		assert.AlwaysOrUnreachable(schedResp.GetCron() == "0 * * * *",
			"chapter schedule should match what was set",
			details.With(internal.Details{"actual": schedResp.GetCron()}))

		// 3. Delete the schedule.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_DeleteChapterSchedule{
				DeleteChapterSchedule: &servicepb.DeleteChapterScheduleRequest{},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to delete chapter schedule", details.With(internal.Details{"error": err}))
	})
}
