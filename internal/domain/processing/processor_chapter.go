package processing

import (
	"bytes"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processCloseChapter handles the CloseChapter order. It transitions the
// current OPEN chapter to CLOSING and creates a new OPEN chapter. The
// CloseChapter intent (LastAuditHash carry after the audit entry hashes)
// is derived from the ClosedChapterLog by deriveSignals — the processor
// only mutates state and returns the log.
func processCloseChapter(_ *raftcmdpb.CloseChapterOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
	currentReader, ok := s.GetCurrentOpenChapter()
	if !ok {
		return nil, domain.ErrNoChapterOpen
	}

	currentChapter := currentReader.Mutate()

	// Transition current chapter to CLOSING
	currentChapter.Status = commonpb.ChapterStatus_CHAPTER_CLOSING
	currentChapter.CloseSequence = s.GetNextSequenceID()
	currentChapter.End = s.GetDate().Mutate()
	// The chain input for the first audit entry that survives this chapter's
	// purge, and one of the four fields the sealing hash commits to. It is the
	// head as this proposal began: the entry this proposal writes belongs to the
	// next chapter's range. Written here so it is durable with the close itself —
	// the seal that hashes it can be applied by a process that never saw this
	// apply.
	currentChapter.LastAuditHash = bytes.Clone(s.GetLastAuditHash())
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
		Start:              s.GetDate().Mutate(),
		Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
		StartSequence:      s.GetNextSequenceID() + 1,
		StartAuditSequence: s.GetNextAuditSequenceID(),
	}
	s.SetCurrentOpenChapter(newChapter)

	// Clone the chapter for the log payload so the log's snapshot is immutable.
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
func processSealChapter(order *raftcmdpb.SealChapterOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
	closingReader, ok := s.GetClosingChapterByID(order.GetChapterId())
	if !ok {
		return nil, &domain.ErrChapterNotFound{ChapterID: order.GetChapterId()}
	}

	if closingReader.GetStatus() != commonpb.ChapterStatus_CHAPTER_CLOSING {
		return nil, &domain.ErrChapterNotClosing{ChapterID: order.GetChapterId()}
	}

	closingChapter := closingReader.Mutate()

	// Transition to CLOSED and remove from closing chapters
	closingChapter.Status = commonpb.ChapterStatus_CHAPTER_CLOSED
	closingChapter.SealingHash = order.GetSealingHash()
	closingChapter.StateHash = order.GetStateHash()

	// Mutate() returned a clone — rebind the buffer to it so the FSM
	// cache + the changedChapters auto-record in RemoveClosingChapter
	// observe the CLOSED state instead of the pre-mutation pointer.
	s.UpdateChapter(closingChapter)
	s.RemoveClosingChapter(order.GetChapterId())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SealChapter{
			SealChapter: &commonpb.SealedChapterLog{
				Chapter: closingChapter,
			},
		},
	}, nil
}

// processArchiveChapter handles the ArchiveChapter order. It transitions
// the chapter CLOSED → ARCHIVING. The archive worker request is derived
// from the ArchivedChapterLog by deriveSignals (Chapter carries every
// sequence range the worker needs).
func processArchiveChapter(order *raftcmdpb.ArchiveChapterOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	// Archived chapters must form a contiguous prefix of history, so the only
	// archivable chapter is the one right after the archived prefix. The purge
	// deletes everything up to the archived chapter's close sequence, so
	// archiving out of order would leave an older, un-archived chapter's logs
	// retained *below* the archive boundary — where the checker's replay skips
	// them, leaving a permanently unverified window, and where cold storage has
	// a hole no later archive ever fills.
	//
	// The prefix comes from the tracker's explicit marker, and chapters inside it
	// are rejected BEFORE the residency lookup below: an archived chapter is
	// absent from the tracker on a running node (the purge drops it) and present
	// with status ARCHIVED after a restart (recovery reloads its row), so a
	// lookup-first order would answer the same Raft order with two different
	// reasons — CHAPTER_NOT_FOUND or CHAPTER_NOT_CLOSED — and the audited
	// failures, which are hash-chained, would diverge across replicas.
	archivedThrough := s.GetArchivedThroughChapterID()
	if order.GetChapterId() <= archivedThrough {
		return nil, &domain.ErrChapterAlreadyArchived{
			ChapterID:                order.GetChapterId(),
			ArchivedThroughChapterID: archivedThrough,
		}
	}

	// Past the prefix the lookup is representation-independent: a chapter that is
	// not archived is never purged, so it is resident on every node, and one that
	// never existed has no row anywhere.
	chapterReader, ok := s.GetChapterByID(order.GetChapterId())
	if !ok {
		return nil, &domain.ErrChapterNotFound{ChapterID: order.GetChapterId()}
	}

	if chapterReader.GetStatus() != commonpb.ChapterStatus_CHAPTER_CLOSED {
		return nil, &domain.ErrChapterNotClosed{ChapterID: order.GetChapterId()}
	}

	if order.GetChapterId() != archivedThrough+1 {
		return nil, &domain.ErrChapterArchiveOutOfOrder{
			ChapterID:         order.GetChapterId(),
			BlockingChapterID: archivedThrough + 1,
		}
	}

	chapter := chapterReader.Mutate()

	// Transition to ARCHIVING deterministically on all nodes
	chapter.Status = commonpb.ChapterStatus_CHAPTER_ARCHIVING
	s.UpdateChapter(chapter)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ArchiveChapter{
			ArchiveChapter: &commonpb.ArchivedChapterLog{
				Chapter: chapter,
			},
		},
	}, nil
}

// processConfirmArchiveChapter handles the ConfirmArchiveChapter order. It
// transitions an ARCHIVING chapter to ARCHIVED. The purge range is derived
// from the ConfirmedArchiveChapterLog by deriveSignals.
func processConfirmArchiveChapter(order *raftcmdpb.ConfirmArchiveChapterOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	// The confirm is what extends the archived prefix and triggers the purge, so
	// it must land exactly on prefix + 1 — checked before any mutation, and
	// before the residency lookup for the same reason as processArchiveChapter
	// (an already-archived chapter reads as absent on a running node and as
	// ARCHIVED after a restart, which would audit two different reasons for one
	// order).
	//
	// A confirm past the prefix is not merely redundant: the recovery derivation
	// stops at a gap while DispatchArchiveRequests republishes every ARCHIVING
	// chapter, so a store holding {1 ARCHIVED, 2 un-archived, 3 ARCHIVING} would
	// otherwise have reconciliation confirm 3 and carry the prefix past 2 —
	// re-authorising archives while chapter 2's logs sit un-archived below the
	// resulting archive boundary. That state is impossible through the ordering
	// gate, so refusing it surfaces the anomaly (audited failure, and the
	// archiver retries visibly) instead of silently papering over it.
	archivedThrough := s.GetArchivedThroughChapterID()
	if order.GetChapterId() <= archivedThrough {
		return nil, &domain.ErrChapterAlreadyArchived{
			ChapterID:                order.GetChapterId(),
			ArchivedThroughChapterID: archivedThrough,
		}
	}

	if order.GetChapterId() != archivedThrough+1 {
		return nil, &domain.ErrChapterArchiveOutOfOrder{
			ChapterID:         order.GetChapterId(),
			BlockingChapterID: archivedThrough + 1,
		}
	}

	chapterReader, ok := s.GetChapterByID(order.GetChapterId())
	if !ok {
		return nil, &domain.ErrChapterNotFound{ChapterID: order.GetChapterId()}
	}

	if chapterReader.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVING {
		return nil, &domain.ErrChapterNotArchiving{ChapterID: order.GetChapterId()}
	}

	// The confirm trades hot history for a cold archive, so it must name the
	// incarnation the archive was built for. Ranges alone do not identify one: a
	// store restored from an older backup over a surviving cold-storage namespace
	// can reuse a chapter id and reach the same log and audit counts over different
	// operations, so confirming against the stale archive would purge history that
	// archive does not contain. The sealing hash commits a chapter to its content,
	// and comparing it here — in apply, against the chapter the FSM holds — puts the
	// check in the same step as the purge it authorises.
	if !bytes.Equal(order.GetSealingHash(), chapterReader.GetSealingHash()) {
		return nil, &domain.ErrChapterArchiveIdentityMismatch{
			ChapterID: order.GetChapterId(),
			Expected:  chapterReader.GetSealingHash(),
			Got:       order.GetSealingHash(),
		}
	}

	chapter := chapterReader.Mutate()

	chapter.Status = commonpb.ChapterStatus_CHAPTER_ARCHIVED
	s.UpdateChapter(chapter)

	// Advancing here rather than at purge time means a later order in the same
	// proposal sees it. The Scope exposes only a one-step advance, so no caller
	// can carry the prefix over a gap.
	s.AdvanceArchivedThroughChapterID()

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ConfirmArchiveChapter{
			ConfirmArchiveChapter: &commonpb.ConfirmedArchiveChapterLog{
				Chapter: chapter,
			},
		},
	}, nil
}
