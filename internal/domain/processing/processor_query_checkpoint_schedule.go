package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

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
