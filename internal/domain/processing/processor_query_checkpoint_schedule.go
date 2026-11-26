package processing

import (
	"github.com/robfig/cron/v3"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// CronParser accepts both the standard 5-field format (minute-level) and the
// extended 6-field format with an optional leading seconds field.
// It is exported so the QueryCheckpointScheduler can reuse the same parser.
var CronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// processSetQueryCheckpointSchedule handles the SetQueryCheckpointSchedule order.
// It validates the cron expression; the schedule-set signal is derived
// from the produced log by deriveSignals.
func processSetQueryCheckpointSchedule(order *raftcmdpb.SetQueryCheckpointScheduleOrder, _ *Context) (*commonpb.LogPayload, domain.Describable) {
	if _, err := CronParser.Parse(order.GetCron()); err != nil {
		return nil, &domain.ErrInvalidCronExpression{
			Expression: order.GetCron(),
			Details:    err.Error(),
		}
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetQueryCheckpointSchedule{
			SetQueryCheckpointSchedule: &commonpb.SetQueryCheckpointScheduleLog{
				Cron: order.GetCron(),
			},
		},
	}, nil
}

// processDeleteQueryCheckpointSchedule handles the DeleteQueryCheckpointSchedule order.
// The framework derives the schedule-deleted signal from the log.
func processDeleteQueryCheckpointSchedule(_ *Context) (*commonpb.LogPayload, domain.Describable) {
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteQueryCheckpointSchedule{
			DeleteQueryCheckpointSchedule: &commonpb.DeletedQueryCheckpointScheduleLog{},
		},
	}, nil
}
