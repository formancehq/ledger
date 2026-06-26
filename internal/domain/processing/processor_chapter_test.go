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

	mockStore.EXPECT().GetChapterByID(uint64(1)).Return(openChapter.AsReader(), true)

	payload, err := processArchiveChapter(&raftcmdpb.ArchiveChapterOrder{ChapterId: 1}, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notClosedErr *domain.ErrChapterNotClosed
	require.ErrorAs(t, err, &notClosedErr)
	require.Equal(t, uint64(1), notClosedErr.ChapterID)
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

	mockStore.EXPECT().GetChapterByID(uint64(1)).Return(archivingChapter.AsReader(), true)
	mockStore.EXPECT().UpdateChapter(gomock.Any()).Do(func(chapter *commonpb.Chapter) {
		require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVED, chapter.GetStatus())
	})

	payload, err := processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{ChapterId: 1}, &Context{Scope: mockStore})
	require.NoError(t, err)
	require.NotNil(t, payload)

	confirmLog := payload.GetConfirmArchiveChapter()
	require.NotNil(t, confirmLog)
	require.Equal(t, uint64(1), confirmLog.GetChapter().GetId())
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_ARCHIVED, confirmLog.GetChapter().GetStatus())
}

func TestProcessConfirmArchiveChapter_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

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

	archivedChapter := &commonpb.Chapter{
		Id:     1,
		Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED,
	}

	mockStore.EXPECT().GetChapterByID(uint64(1)).Return(archivedChapter.AsReader(), true)

	payload, err := processConfirmArchiveChapter(&raftcmdpb.ConfirmArchiveChapterOrder{ChapterId: 1}, &Context{Scope: mockStore})
	require.Error(t, err)
	require.Nil(t, payload)

	var notArchivingErr *domain.ErrChapterNotArchiving
	require.ErrorAs(t, err, &notArchivingErr)
	require.Equal(t, uint64(1), notArchivingErr.ChapterID)
}
