package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessClosePeriod_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1700000000}
	openPeriod := &commonpb.Period{
		Id:     1,
		Start:  &commonpb.Timestamp{Data: 1699000000},
		Status: commonpb.PeriodStatus_PERIOD_OPEN,
	}

	mockStore.EXPECT().GetCurrentOpenPeriod().Return(openPeriod, true)
	// GetNextSequenceID is called twice: once for CloseSequence, once for StartSequence
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(42)).Times(2)
	mockStore.EXPECT().GetDate().Return(now).Times(2)
	mockStore.EXPECT().GetNextAuditSequenceID().Return(uint64(10)).Times(2)
	mockStore.EXPECT().IncrementNextPeriodID().Return(uint64(2))
	mockStore.EXPECT().AddClosingPeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSING, period.GetStatus())
		require.Equal(t, uint64(42), period.GetCloseSequence())
		require.Equal(t, uint64(9), period.GetCloseAuditSequence()) // nextAuditSeq - 1
	})
	mockStore.EXPECT().SetCurrentOpenPeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, period.GetStatus())
		require.Equal(t, uint64(2), period.GetId())
		require.Equal(t, uint64(43), period.GetStartSequence())      // CloseSequence + 1
		require.Equal(t, uint64(10), period.GetStartAuditSequence()) // nextAuditSeq
	})

	payload, err := processor.processClosePeriod(&raftcmdpb.ClosePeriodOrder{}, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	closePeriodLog := payload.GetClosePeriod()
	require.NotNil(t, closePeriodLog)
	require.Equal(t, uint64(1), closePeriodLog.GetClosedPeriod().GetId())
	require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSING, closePeriodLog.GetClosedPeriod().GetStatus())
	require.Equal(t, uint64(2), closePeriodLog.GetNewPeriod().GetId())
	require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, closePeriodLog.GetNewPeriod().GetStatus())
}

func TestProcessClosePeriod_NoPeriodOpen(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)

	payload, err := processor.processClosePeriod(&raftcmdpb.ClosePeriodOrder{}, mockStore)
	require.ErrorIs(t, err, domain.ErrNoPeriodOpen)
	require.Nil(t, payload)
}

func TestProcessClosePeriod_SucceedsWhileAnotherPeriodIsClosing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1700000000}
	openPeriod := &commonpb.Period{
		Id:     2,
		Start:  &commonpb.Timestamp{Data: 1699500000},
		Status: commonpb.PeriodStatus_PERIOD_OPEN,
	}

	// Another period is already closing — this should NOT prevent the new close
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(openPeriod, true)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100)).Times(2)
	mockStore.EXPECT().GetDate().Return(now).Times(2)
	mockStore.EXPECT().GetNextAuditSequenceID().Return(uint64(20)).Times(2)
	mockStore.EXPECT().IncrementNextPeriodID().Return(uint64(3))
	mockStore.EXPECT().AddClosingPeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, uint64(2), period.GetId())
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSING, period.GetStatus())
	})
	mockStore.EXPECT().SetCurrentOpenPeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, uint64(3), period.GetId())
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, period.GetStatus())
	})

	payload, err := processor.processClosePeriod(&raftcmdpb.ClosePeriodOrder{}, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	closePeriodLog := payload.GetClosePeriod()
	require.NotNil(t, closePeriodLog)
	require.Equal(t, uint64(2), closePeriodLog.GetClosedPeriod().GetId())
	require.Equal(t, uint64(3), closePeriodLog.GetNewPeriod().GetId())
}

func TestProcessSealPeriod_SealsOneWhileOthersRemain(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// Two periods are closing; we seal the first one
	targetPeriod := &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_CLOSING,
		CloseSequence: 42,
	}

	mockStore.EXPECT().GetClosingPeriodByID(uint64(1)).Return(targetPeriod, true)
	mockStore.EXPECT().RemoveClosingPeriod(uint64(1))

	order := &raftcmdpb.SealPeriodOrder{
		PeriodId:    1,
		SealingHash: []byte("seal-hash-1"),
		StateHash:   []byte("state-hash-1"),
	}

	payload, err := processor.processSealPeriod(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	sealLog := payload.GetSealPeriod()
	require.NotNil(t, sealLog)
	require.Equal(t, uint64(1), sealLog.GetPeriod().GetId())
	require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, sealLog.GetPeriod().GetStatus())
}

func TestProcessSealPeriod_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	closingPeriod := &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_CLOSING,
		CloseSequence: 42,
	}

	mockStore.EXPECT().GetClosingPeriodByID(uint64(1)).Return(closingPeriod, true)
	mockStore.EXPECT().RemoveClosingPeriod(uint64(1))

	order := &raftcmdpb.SealPeriodOrder{
		PeriodId:    1,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processor.processSealPeriod(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	sealPeriodLog := payload.GetSealPeriod()
	require.NotNil(t, sealPeriodLog)
	require.Equal(t, uint64(1), sealPeriodLog.GetPeriod().GetId())
	require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, sealPeriodLog.GetPeriod().GetStatus())
	require.Equal(t, []byte("seal-hash"), sealPeriodLog.GetPeriod().GetSealingHash())
}

func TestProcessSealPeriod_PeriodNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// No closing period with this ID
	mockStore.EXPECT().GetClosingPeriodByID(uint64(99)).Return(nil, false)

	order := &raftcmdpb.SealPeriodOrder{
		PeriodId:    99,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processor.processSealPeriod(order, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrPeriodNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(99), notFoundErr.PeriodID)
}

func TestProcessSealPeriod_PeriodNotClosing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// The closing period exists but has wrong ID — use GetClosingPeriodByID which returns not found
	mockStore.EXPECT().GetClosingPeriodByID(uint64(1)).Return(nil, false)

	order := &raftcmdpb.SealPeriodOrder{
		PeriodId:    1,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processor.processSealPeriod(order, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrPeriodNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(1), notFoundErr.PeriodID)
}

func TestProcessArchivePeriod_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	closedPeriod := &commonpb.Period{
		Id:                 1,
		Status:             commonpb.PeriodStatus_PERIOD_CLOSED,
		StartSequence:      1,
		CloseSequence:      42,
		StartAuditSequence: 3,
		CloseAuditSequence: 17,
		SealingHash:        []byte("seal-hash"),
	}

	mockStore.EXPECT().GetPeriodByID(uint64(1)).Return(closedPeriod, true)
	mockStore.EXPECT().UpdatePeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, commonpb.PeriodStatus_PERIOD_ARCHIVING, period.GetStatus())
	})
	mockStore.EXPECT().SetPendingArchive(uint64(1), uint64(1), uint64(42), uint64(3), uint64(17))

	payload, err := processor.processArchivePeriod(&raftcmdpb.ArchivePeriodOrder{PeriodId: 1}, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	archiveLog := payload.GetArchivePeriod()
	require.NotNil(t, archiveLog)
	require.Equal(t, uint64(1), archiveLog.GetPeriod().GetId())
	require.Equal(t, commonpb.PeriodStatus_PERIOD_ARCHIVING, archiveLog.GetPeriod().GetStatus())
}

func TestProcessArchivePeriod_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetPeriodByID(uint64(99)).Return(nil, false)

	payload, err := processor.processArchivePeriod(&raftcmdpb.ArchivePeriodOrder{PeriodId: 99}, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrPeriodNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(99), notFoundErr.PeriodID)
}

func TestProcessArchivePeriod_NotClosed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	openPeriod := &commonpb.Period{
		Id:     1,
		Status: commonpb.PeriodStatus_PERIOD_OPEN,
	}

	mockStore.EXPECT().GetPeriodByID(uint64(1)).Return(openPeriod, true)

	payload, err := processor.processArchivePeriod(&raftcmdpb.ArchivePeriodOrder{PeriodId: 1}, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notClosedErr *domain.ErrPeriodNotClosed
	require.ErrorAs(t, err, &notClosedErr)
	require.Equal(t, uint64(1), notClosedErr.PeriodID)
}

func TestProcessConfirmArchivePeriod_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	archivingPeriod := &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_ARCHIVING,
		StartSequence: 1,
		CloseSequence: 42,
		SealingHash:   []byte("seal-hash"),
	}

	mockStore.EXPECT().GetPeriodByID(uint64(1)).Return(archivingPeriod, true)
	mockStore.EXPECT().UpdatePeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, commonpb.PeriodStatus_PERIOD_ARCHIVED, period.GetStatus())
	})
	mockStore.EXPECT().SetPurgeRange(uint64(1), uint64(1), uint64(42), gomock.Any(), gomock.Any())

	payload, err := processor.processConfirmArchivePeriod(&raftcmdpb.ConfirmArchivePeriodOrder{PeriodId: 1}, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	confirmLog := payload.GetConfirmArchivePeriod()
	require.NotNil(t, confirmLog)
	require.Equal(t, uint64(1), confirmLog.GetPeriod().GetId())
	require.Equal(t, commonpb.PeriodStatus_PERIOD_ARCHIVED, confirmLog.GetPeriod().GetStatus())
}

func TestProcessConfirmArchivePeriod_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetPeriodByID(uint64(99)).Return(nil, false)

	payload, err := processor.processConfirmArchivePeriod(&raftcmdpb.ConfirmArchivePeriodOrder{PeriodId: 99}, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *domain.ErrPeriodNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(99), notFoundErr.PeriodID)
}

func TestProcessConfirmArchivePeriod_NotArchiving(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	archivedPeriod := &commonpb.Period{
		Id:     1,
		Status: commonpb.PeriodStatus_PERIOD_ARCHIVED,
	}

	mockStore.EXPECT().GetPeriodByID(uint64(1)).Return(archivedPeriod, true)

	payload, err := processor.processConfirmArchivePeriod(&raftcmdpb.ConfirmArchivePeriodOrder{PeriodId: 1}, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notArchivingErr *domain.ErrPeriodNotArchiving
	require.ErrorAs(t, err, &notArchivingErr)
	require.Equal(t, uint64(1), notArchivingErr.PeriodID)
}
