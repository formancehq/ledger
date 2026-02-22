package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/robfig/cron/v3"
)

// CronParser accepts both the standard 5-field format (minute-level) and the
// extended 6-field format with an optional leading seconds field.
// It is exported so the PeriodScheduler can reuse the same parser.
var CronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// processSetPeriodSchedule handles the SetPeriodSchedule order.
// It validates the cron expression and stores it in the FSM state.
func (p *RequestProcessor) processSetPeriodSchedule(order *raftcmdpb.SetPeriodScheduleOrder, s Store) (*commonpb.LogPayload, error) {
	if _, err := CronParser.Parse(order.Cron); err != nil {
		return nil, &ErrInvalidCronExpression{
			Expression: order.Cron,
			Details:    err.Error(),
		}
	}

	s.SetPeriodSchedule(order.Cron)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetPeriodSchedule{
			SetPeriodSchedule: &commonpb.SetPeriodScheduleLog{
				Cron: order.Cron,
			},
		},
	}, nil
}

// processDeletePeriodSchedule handles the DeletePeriodSchedule order.
// It removes the period schedule from the FSM state.
func (p *RequestProcessor) processDeletePeriodSchedule(s Store) (*commonpb.LogPayload, error) {
	s.DeletePeriodSchedule()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletePeriodSchedule{
			DeletePeriodSchedule: &commonpb.DeletePeriodScheduleLog{},
		},
	}, nil
}
