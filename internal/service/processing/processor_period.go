package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

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
	newPeriod := &commonpb.Period{
		Id:     s.IncrementNextPeriodID(),
		Start:  s.GetDate(),
		Status: commonpb.PeriodStatus_PERIOD_OPEN,
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
