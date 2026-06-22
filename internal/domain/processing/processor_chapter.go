package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processCloseChapter handles the CloseChapter order.
// It transitions the current OPEN chapter to CLOSING and creates a new OPEN chapter.
func (p *RequestProcessor) processCloseChapter(_ *raftcmdpb.CloseChapterOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	currentChapter, ok := s.GetCurrentOpenChapter()
	if !ok {
		return nil, domain.ErrNoChapterOpen
	}

	// Transition current chapter to CLOSING
	currentChapter.Status = commonpb.ChapterStatus_CHAPTER_CLOSING
	currentChapter.CloseSequence = s.GetNextSequenceID()
	currentChapter.End = s.GetDate()
	// LastAuditHash is set later in applyProposal after the audit hash is computed.
	// Capture the audit sequence at close time. The next audit sequence ID is
	// one past the last written, so close_audit_sequence = next - 1.
	// If no audit entries were written (nextAudit == startAudit), close equals
	// start - 1, which makes the purge range empty (correct: nothing to purge).
	currentChapter.CloseAuditSequence = s.GetNextAuditSequenceID() - 1
	s.AddClosingChapter(currentChapter)

	// Create new OPEN chapter
	// StartSequence is the next sequence after the close boundary (close_sequence is the CloseChapter log itself)
	newChapter := &commonpb.Chapter{
		Id:                 s.IncrementNextChapterID(),
		Start:              s.GetDate(),
		Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
		StartSequence:      s.GetNextSequenceID() + 1,
		StartAuditSequence: s.GetNextAuditSequenceID(),
	}
	s.SetCurrentOpenChapter(newChapter)

	// Clone the chapter for the log payload so the log's snapshot is immutable.
	// applyProposal will set LastAuditHash on the FSM chapter after computing
	// the batch-level audit hash.
	closedChapterSnapshot := currentChapter.CloneVT()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CloseChapter{
			CloseChapter: &commonpb.ClosedChapterLog{
				ClosedChapter: closedChapterSnapshot,
				NewChapter:    newChapter,
			},
		},
	}, nil
}

// processSealChapter handles the SealChapter order.
// It transitions a CLOSING chapter to CLOSED and sets the sealing hash.
func (p *RequestProcessor) processSealChapter(order *raftcmdpb.SealChapterOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	closingChapter, ok := s.GetClosingChapterByID(order.GetChapterId())
	if !ok {
		return nil, &domain.ErrChapterNotFound{ChapterID: order.GetChapterId()}
	}

	if closingChapter.GetStatus() != commonpb.ChapterStatus_CHAPTER_CLOSING {
		return nil, &domain.ErrChapterNotClosing{ChapterID: order.GetChapterId()}
	}

	// Transition to CLOSED and remove from closing chapters
	closingChapter.Status = commonpb.ChapterStatus_CHAPTER_CLOSED
	closingChapter.SealingHash = order.GetSealingHash()
	closingChapter.StateHash = order.GetStateHash()

	s.RemoveClosingChapter(order.GetChapterId())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SealChapter{
			SealChapter: &commonpb.SealedChapterLog{
				Chapter: closingChapter,
			},
		},
	}, nil
}

// processArchiveChapter handles the ArchiveChapter order.
// It transitions the chapter from CLOSED → ARCHIVING and returns an ArchivedChapterLog
// to signal the background Archiver (leader-only dispatch happens in Node).
func (p *RequestProcessor) processArchiveChapter(order *raftcmdpb.ArchiveChapterOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	chapter, ok := s.GetChapterByID(order.GetChapterId())
	if !ok {
		return nil, &domain.ErrChapterNotFound{ChapterID: order.GetChapterId()}
	}

	if chapter.GetStatus() != commonpb.ChapterStatus_CHAPTER_CLOSED {
		return nil, &domain.ErrChapterNotClosed{ChapterID: order.GetChapterId()}
	}

	// Transition to ARCHIVING deterministically on all nodes
	chapter.Status = commonpb.ChapterStatus_CHAPTER_ARCHIVING
	s.UpdateChapter(chapter)

	// Signal the Machine to send an archive request after batch commit
	s.SetPendingArchive(chapter.GetId(), chapter.GetStartSequence(), chapter.GetCloseSequence(), chapter.GetStartAuditSequence(), chapter.GetCloseAuditSequence())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ArchiveChapter{
			ArchiveChapter: &commonpb.ArchivedChapterLog{
				Chapter: chapter,
			},
		},
	}, nil
}

// processConfirmArchiveChapter handles the ConfirmArchiveChapter order.
// It transitions an ARCHIVING chapter to ARCHIVED and signals a purge of logs and audit entries.
func (p *RequestProcessor) processConfirmArchiveChapter(order *raftcmdpb.ConfirmArchiveChapterOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	chapter, ok := s.GetChapterByID(order.GetChapterId())
	if !ok {
		return nil, &domain.ErrChapterNotFound{ChapterID: order.GetChapterId()}
	}

	if chapter.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVING {
		return nil, &domain.ErrChapterNotArchiving{ChapterID: order.GetChapterId()}
	}

	chapter.Status = commonpb.ChapterStatus_CHAPTER_ARCHIVED
	s.UpdateChapter(chapter)

	// Signal the FSM to purge logs and audit entries for this chapter's sequence ranges.
	// Logs and audit entries have independent sequence counters, so both ranges are needed.
	s.SetPurgeRange(chapter.GetId(), chapter.GetStartSequence(), chapter.GetCloseSequence(),
		chapter.GetStartAuditSequence(), chapter.GetCloseAuditSequence())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ConfirmArchiveChapter{
			ConfirmArchiveChapter: &commonpb.ConfirmedArchiveChapterLog{
				Chapter: chapter,
			},
		},
	}, nil
}
