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

// processClosePeriod handles the ClosePeriod order.
// It transitions the current OPEN period to CLOSING and creates a new OPEN period.
func (p *RequestProcessor) processClosePeriod(_ *raftcmdpb.ClosePeriodOrder, s Store) (*commonpb.LogPayload, error) {
	currentPeriod, ok := s.GetCurrentOpenPeriod()
	if !ok {
		return nil, ErrNoPeriodOpen
	}

	if _, hasClosing := s.GetClosingPeriod(); hasClosing {
		return nil, ErrPeriodAlreadyClosing
	}

	// Transition current period to CLOSING
	currentPeriod.Status = commonpb.PeriodStatus_PERIOD_CLOSING
	currentPeriod.CloseSequence = s.GetNextSequenceID()
	currentPeriod.End = s.GetDate()
	currentPeriod.LastLogHash = s.GetLastLogHash()
	s.SetClosingPeriod(currentPeriod)

	// Create new OPEN period
	// StartSequence is the next sequence after the close boundary (close_sequence is the ClosePeriod log itself)
	newPeriod := &commonpb.Period{
		Id:            s.IncrementNextPeriodID(),
		Start:         s.GetDate(),
		Status:        commonpb.PeriodStatus_PERIOD_OPEN,
		StartSequence: s.GetNextSequenceID() + 1,
	}
	s.SetCurrentOpenPeriod(newPeriod)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ClosePeriod{
			ClosePeriod: &commonpb.ClosePeriodLog{
				ClosedPeriod: currentPeriod,
				NewPeriod:    newPeriod,
			},
		},
	}, nil
}

// processSealPeriod handles the SealPeriod order.
// It transitions a CLOSING period to CLOSED and sets the sealing hash.
func (p *RequestProcessor) processSealPeriod(order *raftcmdpb.SealPeriodOrder, s Store) (*commonpb.LogPayload, error) {
	closingPeriod, ok := s.GetClosingPeriod()
	if !ok || closingPeriod.Id != order.PeriodId {
		return nil, &ErrPeriodNotFound{PeriodID: order.PeriodId}
	}

	if closingPeriod.Status != commonpb.PeriodStatus_PERIOD_CLOSING {
		return nil, &ErrPeriodNotClosing{PeriodID: order.PeriodId}
	}

	// Transition to CLOSED and persist via ClearClosingPeriod
	closingPeriod.Status = commonpb.PeriodStatus_PERIOD_CLOSED
	closingPeriod.SealingHash = order.SealingHash
	s.ClearClosingPeriod()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SealPeriod{
			SealPeriod: &commonpb.SealPeriodLog{
				Period: closingPeriod,
			},
		},
	}, nil
}

// processArchivePeriod handles the ArchivePeriod order.
// It transitions the period from CLOSED → ARCHIVING and returns an ArchivePeriodLog
// to signal the background Archiver (leader-only dispatch happens in Node).
func (p *RequestProcessor) processArchivePeriod(order *raftcmdpb.ArchivePeriodOrder, s Store) (*commonpb.LogPayload, error) {
	period, ok := s.GetPeriodByID(order.PeriodId)
	if !ok {
		return nil, &ErrPeriodNotFound{PeriodID: order.PeriodId}
	}

	if period.Status != commonpb.PeriodStatus_PERIOD_CLOSED {
		return nil, &ErrPeriodNotClosed{PeriodID: order.PeriodId}
	}

	// Transition to ARCHIVING deterministically on all nodes
	period.Status = commonpb.PeriodStatus_PERIOD_ARCHIVING
	s.UpdatePeriod(period)

	// Signal the Machine to send an archive request after batch commit
	s.SetPendingArchive(period.Id, period.StartSequence, period.CloseSequence)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ArchivePeriod{
			ArchivePeriod: &commonpb.ArchivePeriodLog{
				Period: period,
			},
		},
	}, nil
}

// processConfirmArchivePeriod handles the ConfirmArchivePeriod order.
// It transitions an ARCHIVING period to ARCHIVED and signals a purge of logs and audit entries.
func (p *RequestProcessor) processConfirmArchivePeriod(order *raftcmdpb.ConfirmArchivePeriodOrder, s Store) (*commonpb.LogPayload, error) {
	period, ok := s.GetPeriodByID(order.PeriodId)
	if !ok {
		return nil, &ErrPeriodNotFound{PeriodID: order.PeriodId}
	}

	if period.Status != commonpb.PeriodStatus_PERIOD_ARCHIVING {
		return nil, &ErrPeriodNotArchiving{PeriodID: order.PeriodId}
	}

	period.Status = commonpb.PeriodStatus_PERIOD_ARCHIVED
	s.UpdatePeriod(period)

	// Signal the FSM to purge logs and audit entries for this period's sequence range
	s.SetPurgeRange(period.Id, period.StartSequence, period.CloseSequence)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ConfirmArchivePeriod{
			ConfirmArchivePeriod: &commonpb.ConfirmArchivePeriodLog{
				Period: period,
			},
		},
	}, nil
}

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
