package processing

import (
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessClosePeriod_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1700000000}
	openPeriod := &commonpb.Period{
		Id:     1,
		Start:  &commonpb.Timestamp{Data: 1699000000},
		Status: commonpb.PeriodStatus_PERIOD_OPEN,
	}

	lastLogHash := []byte("test-log-hash")

	mockStore.EXPECT().GetCurrentOpenPeriod().Return(openPeriod, true)
	mockStore.EXPECT().GetClosingPeriod().Return(nil, false)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(42))
	mockStore.EXPECT().GetDate().Return(now).Times(2)
	mockStore.EXPECT().GetLastLogHash().Return(lastLogHash)
	mockStore.EXPECT().IncrementNextPeriodID().Return(uint64(2))
	mockStore.EXPECT().SetClosingPeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSING, period.Status)
		require.Equal(t, uint64(42), period.CloseSequence)
		require.Equal(t, lastLogHash, period.LastLogHash)
	})
	mockStore.EXPECT().SetCurrentOpenPeriod(gomock.Any()).Do(func(period *commonpb.Period) {
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, period.Status)
		require.Equal(t, uint64(2), period.Id)
	})

	payload, err := processor.processClosePeriod(&raftcmdpb.ClosePeriodOrder{}, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	closePeriodLog := payload.GetClosePeriod()
	require.NotNil(t, closePeriodLog)
	require.Equal(t, uint64(1), closePeriodLog.ClosedPeriod.Id)
	require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSING, closePeriodLog.ClosedPeriod.Status)
	require.Equal(t, uint64(2), closePeriodLog.NewPeriod.Id)
	require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, closePeriodLog.NewPeriod.Status)
}

func TestProcessClosePeriod_NoPeriodOpen(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)

	payload, err := processor.processClosePeriod(&raftcmdpb.ClosePeriodOrder{}, mockStore)
	require.ErrorIs(t, err, ErrNoPeriodOpen)
	require.Nil(t, payload)
}

func TestProcessClosePeriod_AlreadyClosing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	openPeriod := &commonpb.Period{
		Id:     2,
		Status: commonpb.PeriodStatus_PERIOD_OPEN,
	}
	closingPeriod := &commonpb.Period{
		Id:     1,
		Status: commonpb.PeriodStatus_PERIOD_CLOSING,
	}

	mockStore.EXPECT().GetCurrentOpenPeriod().Return(openPeriod, true)
	mockStore.EXPECT().GetClosingPeriod().Return(closingPeriod, true)

	payload, err := processor.processClosePeriod(&raftcmdpb.ClosePeriodOrder{}, mockStore)
	require.ErrorIs(t, err, ErrPeriodAlreadyClosing)
	require.Nil(t, payload)
}

func TestProcessSealPeriod_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	closingPeriod := &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_CLOSING,
		CloseSequence: 42,
	}

	mockStore.EXPECT().GetClosingPeriod().Return(closingPeriod, true)
	mockStore.EXPECT().ClearClosingPeriod()

	order := &raftcmdpb.SealPeriodOrder{
		PeriodId:    1,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processor.processSealPeriod(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)

	sealPeriodLog := payload.GetSealPeriod()
	require.NotNil(t, sealPeriodLog)
	require.Equal(t, uint64(1), sealPeriodLog.Period.Id)
	require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, sealPeriodLog.Period.Status)
	require.Equal(t, []byte("seal-hash"), sealPeriodLog.Period.SealingHash)
}

func TestProcessSealPeriod_PeriodNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	// No closing period
	mockStore.EXPECT().GetClosingPeriod().Return(nil, false)

	order := &raftcmdpb.SealPeriodOrder{
		PeriodId:    99,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processor.processSealPeriod(order, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *ErrPeriodNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(99), notFoundErr.PeriodID)
}

func TestProcessSealPeriod_PeriodNotClosing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	// The closing period exists but has wrong ID
	closingPeriod := &commonpb.Period{
		Id:     2,
		Status: commonpb.PeriodStatus_PERIOD_CLOSING,
	}

	mockStore.EXPECT().GetClosingPeriod().Return(closingPeriod, true)

	order := &raftcmdpb.SealPeriodOrder{
		PeriodId:    1,
		SealingHash: []byte("seal-hash"),
	}

	payload, err := processor.processSealPeriod(order, mockStore)
	require.Error(t, err)
	require.Nil(t, payload)

	var notFoundErr *ErrPeriodNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(1), notFoundErr.PeriodID)
}

func TestProcessCreateTransaction_PeriodIdInCreatedTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "USD",
	}
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:alice"},
		Asset:      "USD",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, data.ErrNotFound)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(nil, data.ErrNotFound)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(10))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(&commonpb.Period{Id: 5, Status: commonpb.PeriodStatus_PERIOD_OPEN}, true)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world",
								Destination: "users:alice",
								Amount:      commonpb.NewBigInt(big.NewInt(100)),
								Asset:       "USD",
							},
						},
						Force: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(5), createdTx.PeriodId, "PeriodId should match the open period")
	require.Equal(t, uint64(1), createdTx.Transaction.Id)
}

func TestProcessCreateTransaction_PeriodIdZeroWhenNoPeriod(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "USD",
	}
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:bob"},
		Asset:      "USD",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, data.ErrNotFound)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(nil, data.ErrNotFound)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(10))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world",
								Destination: "users:bob",
								Amount:      commonpb.NewBigInt(big.NewInt(50)),
								Asset:       "USD",
							},
						},
						Force: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(0), createdTx.PeriodId, "PeriodId should be 0 when no open period exists")
}
