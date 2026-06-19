package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processSetQueryCheckpointSchedule handles the SetQueryCheckpointSchedule order.
// It validates the cron expression and stores it in the FSM state.
func (p *RequestProcessor) processSetQueryCheckpointSchedule(order *raftcmdpb.SetQueryCheckpointScheduleOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	if _, err := CronParser.Parse(order.GetCron()); err != nil {
		return nil, &domain.ErrInvalidCronExpression{
			Expression: order.GetCron(),
			Details:    err.Error(),
		}
	}

	s.SetQueryCheckpointSchedule(order.GetCron())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetQueryCheckpointSchedule{
			SetQueryCheckpointSchedule: &commonpb.SetQueryCheckpointScheduleLog{
				Cron: order.GetCron(),
			},
		},
	}, nil
}

// processDeleteQueryCheckpointSchedule handles the DeleteQueryCheckpointSchedule order.
// It removes the query checkpoint schedule from the FSM state.
func (p *RequestProcessor) processDeleteQueryCheckpointSchedule(s Scope) (*commonpb.LogPayload, domain.Describable) {
	s.DeleteQueryCheckpointSchedule()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteQueryCheckpointSchedule{
			DeleteQueryCheckpointSchedule: &commonpb.DeletedQueryCheckpointScheduleLog{},
		},
	}, nil
}
