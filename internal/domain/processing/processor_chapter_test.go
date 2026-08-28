package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessCloseChapter_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	now := &commonpb.Timestamp{Data: 1700000000}
	openChapter := &commonpb.Chapter{
		Id:     1,
		Start:  &commonpb.Timestamp{Data: 1699000000},
		Status: commonpb.ChapterStatus_CHAPTER_OPEN,
	}

	mockStore.EXPECT().GetCurrentOpenChapter().Return(openChapter.AsReader(), true)
	// GetNextSequenceID is called twice: once for CloseSequence, once for StartSequence
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(42)).Times(2)
	mockStore.EXPECT().GetDate().Return(now.AsReader()).Times(2)
	mockStore.EXPECT().GetNextAuditSequenceID().Return(uint64(10)).Times(2)
	mockStore.EXPECT().IncrementNextChapterID().Return(uint64(2))
	mockStore.EXPECT().AddClosingChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSING, chapter.GetStatus())
		require.Equal(t, uint64(42), chapter.GetCloseSequence())
		require.Equal(t, uint64(9), chapter.GetCloseAuditSequence()) // nextAuditSeq - 1
	})
	mockStore.EXPECT().SetCurrentOpenChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_OPEN, chapter.GetStatus())
		require.Equal(t, uint64(2), chapter.GetId())
		require.Equal(t, uint64(43), chapter.GetStartSequence())      // CloseSequence + 1
		require.Equal(t, uint64(10), chapter.GetStartAuditSequence()) // nextAuditSeq
	})

	payload, err := processCloseChapter(&raftcmdpb.CloseChapterOrder{}, &Context{Scope: mockStore})
	require.NoError(t, err)
	require.NotNil(t, payload)

	closeChapterLog := payload.GetCloseChapter()
	require.NotNil(t, closeChapterLog)
	require.Equal(t, uint64(1), closeChapterLog.GetClosedChapter().GetId())
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSING, closeChapterLog.GetClosedChapter().GetStatus())
	require.Equal(t, uint64(2), closeChapterLog.GetNewChapter().GetId())
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_OPEN, closeChapterLog.GetNewChapter().GetStatus())
}

func TestProcessCloseChapter_NoChapterOpen(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)

	payload, err := processCloseChapter(&raftcmdpb.CloseChapterOrder{}, &Context{Scope: mockStore})
	require.ErrorIs(t, err, domain.ErrNoChapterOpen)
	require.Nil(t, payload)
}

func TestProcessCloseChapter_SucceedsWhileAnotherChapterIsClosing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	now := &commonpb.Timestamp{Data: 1700000000}
	openChapter := &commonpb.Chapter{
		Id:     2,
		Start:  &commonpb.Timestamp{Data: 1699500000},
		Status: commonpb.ChapterStatus_CHAPTER_OPEN,
	}

	// Another chapter is already closing — this should NOT prevent the new close
	mockStore.EXPECT().GetCurrentOpenChapter().Return(openChapter.AsReader(), true)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100)).Times(2)
	mockStore.EXPECT().GetDate().Return(now.AsReader()).Times(2)
	mockStore.EXPECT().GetNextAuditSequenceID().Return(uint64(20)).Times(2)
	mockStore.EXPECT().IncrementNextChapterID().Return(uint64(3))
	mockStore.EXPECT().AddClosingChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
		require.Equal(t, uint64(2), chapter.GetId())
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSING, chapter.GetStatus())
	})
	mockStore.EXPECT().SetCurrentOpenChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
		require.Equal(t, uint64(3), chapter.GetId())
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_OPEN, chapter.GetStatus())
	})

	payload, err := processCloseChapter(&raftcmdpb.CloseChapterOrder{}, &Context{Scope: mockStore})
	require.NoError(t, err)
	require.NotNil(t, payload)

	closeChapterLog := payload.GetCloseChapter()
	require.NotNil(t, closeChapterLog)
	require.Equal(t, uint64(2), closeChapterLog.GetClosedChapter().GetId())
	require.Equal(t, uint64(3), closeChapterLog.GetNewChapter().GetId())
}

func TestProcessSealChapter_SealsOneWhileOthersRemain(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	// Two chapters are closing; we seal the first one
	targetChapter := &commonpb.Chapter{
		Id:            1,
		Status:        commonpb.ChapterStatus_CHAPTER_CLOSING,
		CloseSequence: 42,
	}

	mockStore.EXPECT().GetClosingChapterByID(uint64(1)).Return(targetChapter.AsReader(), true)
	mockStore.EXPECT().UpdateChapter(gomock.Any())
	mockStore.EXPECT().RemoveClosingChapter(uint64(1))

	order := &raftcmdpb.SealChapterOrder{
		ChapterId:   1,
		SealingHash: []byte("seal-hash-1"),
		StateHash:   []byte("state-hash-1"),
	}

	payload, err := processSealChapter(order, &Context{Scope: mockStore})
	require.NoError(t, err)
	require.NotNil(t, payload)

	sealLog := payload.GetSealChapter()
	require.NotNil(t, sealLog)
	require.Equal(t, uint64(1), sealLog.GetChapter().GetId())
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSED, sealLog.GetChapter().GetStatus())
}

func TestProcessSealChapter_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	closingChapter := &commonpb.Chapter{
		Id:            1,
		Status:        commonpb.ChapterStatus_CHAPTER_CLOSING,
		CloseSequence: 42,
	}

	mockStore.EXPECT().GetClosingChapterByID(uint64(1)).Return(closingChapter.AsReader(), true)
	mockStore.EXPECT().UpdateChapter(gomock.Any())
	mockStore.EXPECT().RemoveClosingChapter(uint64(1))

	order := &raftcmdpb.SealChapterOrder{
		ChapterId:   1,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processSealChapter(order, &Context{Scope: mockStore})
	require.NoError(t, err)
	require.NotNil(t, payload)

	sealChapterLog := payload.GetSealChapter()
	require.NotNil(t, sealChapterLog)
	require.Equal(t, uint64(1), sealChapterLog.GetChapter().GetId())
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSED, sealChapterLog.GetChapter().GetStatus())
	require.Equal(t, []byte("seal-hash"), sealChapterLog.GetChapter().GetSealingHash())
}

func TestProcessSealChapter_ChapterNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	// No closing chapter with this ID
	mockStore.EXPECT().GetClosingChapterByID(uint64(99)).Return(nil, false)

	order := &raftcmdpb.SealChapterOrder{
		ChapterId:   99,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processSealChapter(order, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrChapterNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(99), notFoundErr.ChapterID)
}

func TestProcessSealChapter_ChapterNotClosing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	// The closing chapter exists but has wrong ID — use GetClosingChapterByID which returns not found
	mockStore.EXPECT().GetClosingChapterByID(uint64(1)).Return(nil, false)

	order := &raftcmdpb.SealChapterOrder{
		ChapterId:   1,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processSealChapter(order, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrChapterNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(1), notFoundErr.ChapterID)
}

func TestProcessArchiveChapter_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	closedChapter := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_CLOSED,
		StartSequence:      1,
		CloseSequence:      42,
		StartAuditSequence: 3,
		CloseAuditSequence: 17,
		SealingHash:        []byte("seal-hash"),
	}

	mockStore.EXPECT().GetChapterByID(uint64(1)).Return(closedChapter.AsReader(), true)
	// Nothing archived yet, so chapter 1 is the prefix successor.
	mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(0))
	mockStore.EXPECT().UpdateChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVING, chapter.GetStatus())
	})

	payload, err := processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: 1}, &Context{Scope: mockStore})
	require.NoError(t, err)
	require.NotNil(t, payload)

	archiveLog := payload.GetArchiveChapter()
	require.NotNil(t, archiveLog)
	require.Equal(t, uint64(1), archiveLog.GetChapter().GetId())
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVING, archiveLog.GetChapter().GetStatus())
}

func TestProcessArchiveChapter_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(0))
	mockStore.EXPECT().GetChapterByID(uint64(99)).Return(nil, false)

	payload, err := processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: 99}, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrChapterNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(99), notFoundErr.ChapterID)
}

func TestProcessArchiveChapter_NotClosed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	openChapter := &commonpb.Chapter{
		Id:     1,
		Status: commonpb.ChapterStatus_CHAPTER_OPEN,
	}

	mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(0))
	mockStore.EXPECT().GetChapterByID(uint64(1)).Return(openChapter.AsReader(), true)

	payload, err := processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: 1}, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notClosedErr *domain.ErrChapterNotClosed
	require.ErrorAs(t, err, &notClosedErr)
	require.Equal(t, uint64(1), notClosedErr.ChapterID)
}

// TestProcessArchiveChapter_RejectsOutOfOrder pins the archival ordering rule:
// archived chapters form a contiguous prefix of history, so the only archivable
// chapter is the one right after that prefix. Skipping ahead would purge past an
// un-archived chapter, leaving its logs below the archive boundary — a
// permanently unverified window for the checker's replay, and a hole in cold
// storage no later archive fills.
func TestProcessArchiveChapter_RejectsOutOfOrder(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		target          uint64
		archivedThrough uint64
		wantBlockingID  uint64
	}{
		"one chapter ahead of the prefix":   {target: 3, archivedThrough: 1, wantBlockingID: 2},
		"several chapters ahead":            {target: 7, archivedThrough: 2, wantBlockingID: 3},
		"nothing archived yet, skips first": {target: 2, archivedThrough: 0, wantBlockingID: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)

			target := &commonpb.Chapter{
				Id:            tc.target,
				Status:        commonpb.ChapterStatus_CHAPTER_CLOSED,
				StartSequence: 200,
				CloseSequence: 300,
			}

			mockStore.EXPECT().GetArchivedThroughChapterID().Return(tc.archivedThrough)
			mockStore.EXPECT().GetChapterByID(tc.target).Return(target.AsReader(), true).AnyTimes()

			payload, err := processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: tc.target}, &Context{Scope: mockStore})
			require.Error(t, err)
			require.Nil(t, payload, "a rejected archive must emit no log")

			var outOfOrderErr *domain.ErrChapterArchiveOutOfOrder
			require.ErrorAs(t, err, &outOfOrderErr)
			require.Equal(t, tc.target, outOfOrderErr.ChapterID)
			require.Equal(t, tc.wantBlockingID, outOfOrderErr.BlockingChapterID,
				"the rejection must name the chapter that has to be archived first")
		})
	}
}

// TestProcessArchiveChapter_RejectsAlreadyArchived covers the retry path: the
// order names a chapter inside the archived prefix, so it has already been
// carried out. The rejection must say so rather than name a blocking chapter —
// every chapter outside the prefix is newer, and archiving one can never make an
// older, already-archived chapter archivable again, so a client following the
// blocker would walk forward forever.
func TestProcessArchiveChapter_RejectsAlreadyArchived(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		target          uint64
		archivedThrough uint64
	}{
		"oldest chapter in the prefix": {target: 1, archivedThrough: 3},
		"newest chapter in the prefix": {target: 3, archivedThrough: 3},
		"the only archived chapter":    {target: 1, archivedThrough: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)
			mockStore.EXPECT().GetArchivedThroughChapterID().Return(tc.archivedThrough)
			// Ids inside the prefix are rejected before the lookup runs at all: an
			// archived chapter is purged from the tracker on a running node and
			// reloaded as ARCHIVED after a restart, so consulting it would answer the
			// same Raft order with two different reasons.
			mockStore.EXPECT().GetChapterByID(gomock.Any()).Times(0)

			payload, err := processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: tc.target}, &Context{Scope: mockStore})
			require.Error(t, err)
			require.Nil(t, payload, "a rejected archive must emit no log")

			var alreadyArchived *domain.ErrChapterAlreadyArchived
			require.ErrorAs(t, err, &alreadyArchived)
			require.Equal(t, tc.target, alreadyArchived.ChapterID)
			require.Equal(t, tc.archivedThrough, alreadyArchived.ArchivedThroughChapterID,
				"the rejection must report how far the archived prefix reaches")

			var outOfOrder *domain.ErrChapterArchiveOutOfOrder
			require.NotErrorAs(t, err, &outOfOrder, "an already-archived chapter is not waiting on another chapter")
		})
	}
}

// TestProcessArchiveChapter_AllowsPrefixSuccessor is the positive counterpart:
// the chapter immediately after the archived prefix archives normally, whatever
// happened to its predecessors' tracker entries — they are purged (and hence
// absent) on a running node, reloaded as ARCHIVED after a restart, and the gate
// consults neither, only the marker.
func TestProcessArchiveChapter_AllowsPrefixSuccessor(t *testing.T) {
	t.Parallel()

	for name, archivedThrough := range map[string]uint64{
		"first chapter, empty prefix": 0,
		"successor of a long prefix":  9,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)

			targetID := archivedThrough + 1
			target := &commonpb.Chapter{
				Id:            targetID,
				Status:        commonpb.ChapterStatus_CHAPTER_CLOSED,
				StartSequence: 200,
				CloseSequence: 300,
			}

			mockStore.EXPECT().GetChapterByID(targetID).Return(target.AsReader(), true)
			mockStore.EXPECT().GetArchivedThroughChapterID().Return(archivedThrough)
			mockStore.EXPECT().UpdateChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
				require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVING, chapter.GetStatus())
			})

			payload, err := processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: targetID}, &Context{Scope: mockStore})
			require.NoError(t, err)
			require.Equal(t, targetID, payload.GetArchiveChapter().GetChapter().GetId())
		})
	}
}

func TestProcessConfirmArchiveChapter_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	archivingChapter := &commonpb.Chapter{
		Id:            1,
		Status:        commonpb.ChapterStatus_CHAPTER_ARCHIVING,
		StartSequence: 1,
		CloseSequence: 42,
		SealingHash:   []byte("seal-hash"),
	}

	mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(0))
	mockStore.EXPECT().GetChapterByID(uint64(1)).Return(archivingChapter.AsReader(), true)
	mockStore.EXPECT().UpdateChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVED, chapter.GetStatus())
	})
	// The confirm extends the archived prefix, which is what the ordering gate
	// reads to admit the next chapter.
	mockStore.EXPECT().AdvanceArchivedThroughChapterID()

	payload, err := processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{
		ChapterId:   1,
		SealingHash: []byte("seal-hash"),
	}, &Context{Scope: mockStore})
	require.NoError(t, err)
	require.NotNil(t, payload)

	confirmLog := payload.GetConfirmArchiveChapter()
	require.NotNil(t, confirmLog)
	require.Equal(t, uint64(1), confirmLog.GetChapter().GetId())
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVED, confirmLog.GetChapter().GetStatus())
}

// The confirm authorises the purge, so it has to name the incarnation the archive
// was built for. Ranges cannot tell two incarnations apart — a store restored from
// an older backup over a surviving cold-storage namespace can reuse a chapter id
// and reach the same counts over different operations — so the sealing hash is
// what binds the two together, checked in the same step as the purge.
func TestProcessConfirmArchiveChapter_RefusesAnotherIncarnation(t *testing.T) {
	t.Parallel()

	for name, carried := range map[string][]byte{
		"a different incarnation": []byte("other-hash"),
		"no incarnation at all":   nil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)

			archivingChapter := &commonpb.Chapter{
				Id:            1,
				Status:        commonpb.ChapterStatus_CHAPTER_ARCHIVING,
				StartSequence: 1,
				CloseSequence: 42,
				SealingHash:   []byte("seal-hash"),
			}

			// Chapter 1 is the prefix successor, so the continuity check passes and the
			// incarnation check is what rejects the order.
			mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(0))
			mockStore.EXPECT().GetChapterByID(uint64(1)).Return(archivingChapter.AsReader(), true)

			payload, err := processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{
				ChapterId:   1,
				SealingHash: carried,
			}, &Context{Scope: mockStore})

			require.Nil(t, payload)
			require.Equal(t, domain.ErrReasonChapterArchiveIdentityMismatch, err.Reason())
		})
	}
}

func TestProcessConfirmArchiveChapter_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(98))
	mockStore.EXPECT().GetChapterByID(uint64(99)).Return(nil, false)

	payload, err := processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{ChapterId: 99}, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrChapterNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(99), notFoundErr.ChapterID)
}

func TestProcessConfirmArchiveChapter_NotArchiving(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	// The prefix successor, but never archived — a confirm for an already-ARCHIVED
	// chapter is caught earlier by the prefix gate instead.
	closedChapter := &commonpb.Chapter{
		Id:     1,
		Status: commonpb.ChapterStatus_CHAPTER_CLOSED,
	}

	mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(0))
	mockStore.EXPECT().GetChapterByID(uint64(1)).Return(closedChapter.AsReader(), true)

	payload, err := processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{ChapterId: 1}, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notArchivingErr *domain.ErrChapterNotArchiving
	require.ErrorAs(t, err, &notArchivingErr)
	require.Equal(t, uint64(1), notArchivingErr.ChapterID)
}

// TestProcessChapter_ArchivedChapterRejectedIdenticallyAcrossRepresentations is
// the restart/no-restart lock. An archived chapter has two in-memory shapes: it
// is absent from the tracker on a node that has been running since the purge,
// and present with status ARCHIVED on one that recovered from the persisted rows.
// Both must produce the SAME rejection, because the failure is audited and the
// audit entries are hash-chained — two reasons for one Raft order would diverge
// the FSM across replicas.
func TestProcessChapter_ArchivedChapterRejectedIdenticallyAcrossRepresentations(t *testing.T) {
	t.Parallel()

	const (
		archivedThrough = uint64(3)
		target          = uint64(2) // inside the archived prefix
	)

	// Whether the tracker still holds the chapter is exactly what must NOT matter.
	representations := map[string]func(*MockScope){
		"purged from the tracker (running node)": func(m *MockScope) {
			m.EXPECT().GetChapterByID(target).Return(nil, false).AnyTimes()
		},
		"reloaded as ARCHIVED (after restart)": func(m *MockScope) {
			reloaded := &commonpb.Chapter{Id: target, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED}
			m.EXPECT().GetChapterByID(target).Return(reloaded.AsReader(), true).AnyTimes()
		},
	}

	for _, order := range []struct {
		name string
		run  func(Scope) (*commonpb.LogPayload, domain.Describable)
	}{
		{
			name: "ArchiveChapter",
			run: func(s Scope) (*commonpb.LogPayload, domain.Describable) {
				return processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: target}, &Context{Scope: s})
			},
		},
		{
			name: "ConfirmArchiveChapter",
			run: func(s Scope) (*commonpb.LogPayload, domain.Describable) {
				return processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{ChapterId: target}, &Context{Scope: s})
			},
		},
	} {
		t.Run(order.name, func(t *testing.T) {
			t.Parallel()

			reasons := map[string]string{}

			for name, stub := range representations {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				mockStore := NewMockScope(ctrl)
				mockStore.EXPECT().GetArchivedThroughChapterID().Return(archivedThrough)
				stub(mockStore)

				payload, err := order.run(mockStore)
				require.Error(t, err)
				require.Nil(t, payload, "%s: a rejected order must emit no log", name)

				var alreadyArchived *domain.ErrChapterAlreadyArchived
				require.ErrorAs(t, err, &alreadyArchived,
					"%s: an archived chapter must be rejected by the prefix gate, not by the residency lookup", name)
				require.Equal(t, archivedThrough, alreadyArchived.ArchivedThroughChapterID)

				reasons[name] = err.Reason()
			}

			require.Len(t, reasons, 2)

			var distinct []string
			for _, reason := range reasons {
				distinct = append(distinct, reason)
			}

			require.Equal(t, distinct[0], distinct[1],
				"the two representations produced different audited reasons (%v) — the same order would hash differently on a restarted and a non-restarted replica", reasons)
		})
	}
}

// TestProcessConfirmArchiveChapter_RefusesToJumpAGap covers the reconciliation
// path: DispatchArchiveRequests republishes every ARCHIVING chapter, and the
// recovery derivation deliberately stops at a gap, so a store holding
// {1 ARCHIVED, 2 un-archived, 3 ARCHIVING} would otherwise have the confirm for
// 3 carry the prefix past 2 — re-authorising archival while chapter 2's logs sit
// un-archived below the resulting archive boundary.
func TestProcessConfirmArchiveChapter_RefusesToJumpAGap(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	// Chapter 3 is genuinely ARCHIVING, but chapter 2 never got archived, so the
	// prefix stopped at 1.
	archiving := &commonpb.Chapter{Id: 3, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVING}
	mockStore.EXPECT().GetArchivedThroughChapterID().Return(uint64(1))
	mockStore.EXPECT().GetChapterByID(uint64(3)).Return(archiving.AsReader(), true).AnyTimes()

	// No UpdateChapter and no AdvanceArchivedThroughChapterID are stubbed: the
	// order must be refused before any mutation, and the prefix must not move.
	payload, err := processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{ChapterId: 3}, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var outOfOrderErr *domain.ErrChapterArchiveOutOfOrder
	require.ErrorAs(t, err, &outOfOrderErr)
	require.Equal(t, uint64(3), outOfOrderErr.ChapterID)
	require.Equal(t, uint64(2), outOfOrderErr.BlockingChapterID, "the gap must be named")
}
