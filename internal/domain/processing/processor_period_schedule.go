package processing

import (
	"github.com/robfig/cron/v3"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// CronParser accepts both the standard 5-field format (minute-level) and the
// extended 6-field format with an optional leading seconds field.
// It is exported so the PeriodScheduler can reuse the same parser.
var CronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// processSetPeriodSchedule handles the SetPeriodSchedule order.
// It validates the cron expression and stores it in the FSM state.
func (p *RequestProcessor) processSetPeriodSchedule(order *raftcmdpb.SetPeriodScheduleOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	if _, err := CronParser.Parse(order.GetCron()); err != nil {
		return nil, &domain.ErrInvalidCronExpression{
			Expression: order.GetCron(),
			Details:    err.Error(),
		}
	}

	s.SetPeriodSchedule(order.GetCron())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetPeriodSchedule{
			SetPeriodSchedule: &commonpb.SetPeriodScheduleLog{
				Cron: order.GetCron(),
			},
		},
	}, nil
}

// processDeletePeriodSchedule handles the DeletePeriodSchedule order.
// It removes the period schedule from the FSM state.
func (p *RequestProcessor) processDeletePeriodSchedule(s InMemoryStore) (*commonpb.LogPayload, error) {
	s.DeletePeriodSchedule()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletePeriodSchedule{
			DeletePeriodSchedule: &commonpb.DeletedPeriodScheduleLog{},
		},
	}, nil
}
