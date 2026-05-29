package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processClosePeriod handles the ClosePeriod order.
// It transitions the current OPEN period to CLOSING and creates a new OPEN period.
func (p *RequestProcessor) processClosePeriod(_ *raftcmdpb.ClosePeriodOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	currentPeriod, ok := s.GetCurrentOpenPeriod()
	if !ok {
		return nil, domain.ErrNoPeriodOpen
	}

	// Transition current period to CLOSING
	currentPeriod.Status = commonpb.PeriodStatus_PERIOD_CLOSING
	currentPeriod.CloseSequence = s.GetNextSequenceID()
	currentPeriod.End = s.GetDate()
	// LastAuditHash is set later in applyProposal after the audit hash is computed.
	// Capture the audit sequence at close time. The next audit sequence ID is
	// one past the last written, so close_audit_sequence = next - 1.
	// If no audit entries were written (nextAudit == startAudit), close equals
	// start - 1, which makes the purge range empty (correct: nothing to purge).
	currentPeriod.CloseAuditSequence = s.GetNextAuditSequenceID() - 1
	s.AddClosingPeriod(currentPeriod)

	// Create new OPEN period
	// StartSequence is the next sequence after the close boundary (close_sequence is the ClosePeriod log itself)
	newPeriod := &commonpb.Period{
		Id:                 s.IncrementNextPeriodID(),
		Start:              s.GetDate(),
		Status:             commonpb.PeriodStatus_PERIOD_OPEN,
		StartSequence:      s.GetNextSequenceID() + 1,
		StartAuditSequence: s.GetNextAuditSequenceID(),
	}
	s.SetCurrentOpenPeriod(newPeriod)

	// Clone the period for the log payload so the log's snapshot is immutable.
	// applyProposal will set LastAuditHash on the FSM period after computing
	// the batch-level audit hash.
	closedPeriodSnapshot := currentPeriod.CloneVT()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ClosePeriod{
			ClosePeriod: &commonpb.ClosedPeriodLog{
				ClosedPeriod: closedPeriodSnapshot,
				NewPeriod:    newPeriod,
			},
		},
	}, nil
}

// processSealPeriod handles the SealPeriod order.
// It transitions a CLOSING period to CLOSED and sets the sealing hash.
func (p *RequestProcessor) processSealPeriod(order *raftcmdpb.SealPeriodOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	closingPeriod, ok := s.GetClosingPeriodByID(order.GetPeriodId())
	if !ok {
		return nil, &domain.ErrPeriodNotFound{PeriodID: order.GetPeriodId()}
	}

	if closingPeriod.GetStatus() != commonpb.PeriodStatus_PERIOD_CLOSING {
		return nil, &domain.ErrPeriodNotClosing{PeriodID: order.GetPeriodId()}
	}

	// Transition to CLOSED and remove from closing periods
	closingPeriod.Status = commonpb.PeriodStatus_PERIOD_CLOSED
	closingPeriod.SealingHash = order.GetSealingHash()
	closingPeriod.StateHash = order.GetStateHash()

	s.RemoveClosingPeriod(order.GetPeriodId())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SealPeriod{
			SealPeriod: &commonpb.SealedPeriodLog{
				Period: closingPeriod,
			},
		},
	}, nil
}

// processArchivePeriod handles the ArchivePeriod order.
// It transitions the period from CLOSED → ARCHIVING and returns an ArchivedPeriodLog
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
			ArchivePeriod: &commonpb.ArchivedPeriodLog{
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

	// Signal the FSM to purge logs and audit entries for this period's sequence ranges.
	// Logs and audit entries have independent sequence counters, so both ranges are needed.
	s.SetPurgeRange(period.GetId(), period.GetStartSequence(), period.GetCloseSequence(),
		period.GetStartAuditSequence(), period.GetCloseAuditSequence())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ConfirmArchivePeriod{
			ConfirmArchivePeriod: &commonpb.ConfirmedArchivePeriodLog{
				Period: period,
			},
		},
	}, nil
}
