package processing

import (
	"github.com/robfig/cron/v3"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// CronParser accepts both the standard 5-field format (minute-level) and the
// extended 6-field format with an optional leading seconds field.
// It is exported so the ChapterScheduler can reuse the same parser.
var CronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// processSetChapterSchedule handles the SetChapterSchedule order.
// It validates the cron expression and returns the log; deriveSignals
// turns the log into the EmitChapterScheduleSet signal.
func processSetChapterSchedule(order *raftcmdpb.SetChapterScheduleOrder, _ *Context) (*commonpb.LogPayload, domain.Describable) {
	if _, err := CronParser.Parse(order.GetCron()); err != nil {
		return nil, &domain.ErrInvalidCronExpression{
			Expression: order.GetCron(),
			Details:    err.Error(),
		}
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetChapterSchedule{
			SetChapterSchedule: &commonpb.SetChapterScheduleLog{
				Cron: order.GetCron(),
			},
		},
	}, nil
}

// processDeleteChapterSchedule handles the DeleteChapterSchedule order.
// The framework derives the schedule-deleted signal from the log.
func processDeleteChapterSchedule(_ *Context) (*commonpb.LogPayload, domain.Describable) {
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteChapterSchedule{
			DeleteChapterSchedule: &commonpb.DeletedChapterScheduleLog{},
		},
	}, nil
}
