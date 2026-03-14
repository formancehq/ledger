package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// processClosePeriod handles the ClosePeriod order.
// It transitions the current OPEN period to CLOSING and creates a new OPEN period.
func (p *RequestProcessor) processClosePeriod(_ *raftcmdpb.ClosePeriodOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	currentPeriod, ok := s.GetCurrentOpenPeriod()
	if !ok {
		return nil, domain.ErrNoPeriodOpen
	}

	if _, hasClosing := s.GetClosingPeriod(); hasClosing {
		return nil, domain.ErrPeriodAlreadyClosing
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

	// Clone the period for the log payload so the log's snapshot is immutable.
	// processOrders will then update the FSM state's LastLogHash to the
	// ClosePeriod log's hash (needed for CheckStore after archive purge).
	closedPeriodSnapshot := currentPeriod.CloneVT()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ClosePeriod{
			ClosePeriod: &commonpb.ClosePeriodLog{
				ClosedPeriod: closedPeriodSnapshot,
				NewPeriod:    newPeriod,
			},
		},
	}, nil
}

// processSealPeriod handles the SealPeriod order.
// It transitions a CLOSING period to CLOSED and sets the sealing hash.
func (p *RequestProcessor) processSealPeriod(order *raftcmdpb.SealPeriodOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	closingPeriod, ok := s.GetClosingPeriod()
	if !ok || closingPeriod.GetId() != order.GetPeriodId() {
		return nil, &domain.ErrPeriodNotFound{PeriodID: order.GetPeriodId()}
	}

	if closingPeriod.GetStatus() != commonpb.PeriodStatus_PERIOD_CLOSING {
		return nil, &domain.ErrPeriodNotClosing{PeriodID: order.GetPeriodId()}
	}

	// Transition to CLOSED and persist via ClearClosingPeriod
	closingPeriod.Status = commonpb.PeriodStatus_PERIOD_CLOSED
	closingPeriod.SealingHash = order.GetSealingHash()
	closingPeriod.StateHash = order.GetStateHash()

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
func (p *RequestProcessor) processArchivePeriod(order *raftcmdpb.ArchivePeriodOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	period, ok := s.GetPeriodByID(order.GetPeriodId())
	if !ok {
		return nil, &domain.ErrPeriodNotFound{PeriodID: order.GetPeriodId()}
	}

	if period.GetStatus() != commonpb.PeriodStatus_PERIOD_CLOSED {
		return nil, &domain.ErrPeriodNotClosed{PeriodID: order.GetPeriodId()}
	}

	// Transition to ARCHIVING deterministically on all nodes
	period.Status = commonpb.PeriodStatus_PERIOD_ARCHIVING
	s.UpdatePeriod(period)

	// Signal the Machine to send an archive request after batch commit
	s.SetPendingArchive(period.GetId(), period.GetStartSequence(), period.GetCloseSequence())

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
func (p *RequestProcessor) processConfirmArchivePeriod(order *raftcmdpb.ConfirmArchivePeriodOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	period, ok := s.GetPeriodByID(order.GetPeriodId())
	if !ok {
		return nil, &domain.ErrPeriodNotFound{PeriodID: order.GetPeriodId()}
	}

	if period.GetStatus() != commonpb.PeriodStatus_PERIOD_ARCHIVING {
		return nil, &domain.ErrPeriodNotArchiving{PeriodID: order.GetPeriodId()}
	}

	period.Status = commonpb.PeriodStatus_PERIOD_ARCHIVED
	s.UpdatePeriod(period)

	// Signal the FSM to purge logs and audit entries for this period's sequence range
	s.SetPurgeRange(period.GetId(), period.GetStartSequence(), period.GetCloseSequence())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ConfirmArchivePeriod{
			ConfirmArchivePeriod: &commonpb.ConfirmArchivePeriodLog{
				Period: period,
			},
		},
	}, nil
}
