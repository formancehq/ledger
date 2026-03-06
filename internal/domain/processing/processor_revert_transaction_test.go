package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestProcessRevertTransaction_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}

	txKey := domain.TransactionKey{Ledger: "test-ledger", ID: 3}

	// Source had balance: input=1000, output=0
	sourceVol := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(1000),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}
	destVol := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(100),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger"}, true).AnyTimes()
	mockStore.EXPECT().GetReverted(txKey).Return(false, nil)
	mockStore.EXPECT().GetDate().Return(now).Times(4) // ledger date + revert tx timestamps

	// Reversed posting: destination becomes source, source becomes destination
	// Original: bank -> users:123 for 100 USD
	// Revert:   users:123 -> bank for 100 USD
	mockStore.EXPECT().GetVolume(domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "users:123"},
		Asset:      "USD",
	}).Return(sourceVol, nil)
	mockStore.EXPECT().PutVolume(domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "users:123"},
		Asset:      "USD",
	}, gomock.Any())
	mockStore.EXPECT().GetVolume(domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}).Return(destVol, nil)
	mockStore.EXPECT().PutVolume(domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}, gomock.Any())

	mockStore.EXPECT().PutReverted(txKey, true)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(50)).Times(2) // for original TX update + revert TX init
	mockStore.EXPECT().AddTransactionUpdate(txKey, gomock.Any())
	mockStore.EXPECT().AddTransactionUpdate(domain.TransactionKey{Ledger: "test-ledger", ID: 5}, gomock.Any())
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						TransactionId: 3,
						OriginalPostings: []*commonpb.Posting{
							{
								Source:      "bank",
								Destination: "users:123",
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD",
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	revertedTx := applyLog.GetLog().GetData().GetRevertedTransaction()
	require.NotNil(t, revertedTx)
	require.Equal(t, uint64(3), revertedTx.GetRevertedTransactionId())
	require.Equal(t, uint64(5), revertedTx.GetRevertTransaction().GetId())
	require.Len(t, revertedTx.GetRevertTransaction().GetPostings(), 1)

	// Verify posting is reversed
	posting := revertedTx.GetRevertTransaction().GetPostings()[0]
	require.Equal(t, "users:123", posting.GetSource())
	require.Equal(t, "bank", posting.GetDestination())
}

func TestProcessRevertTransaction_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						TransactionId: 99, // Beyond NextTransactionId=5
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var txNotFound *domain.ErrTransactionNotFound
	require.ErrorAs(t, err, &txNotFound)
	require.Equal(t, uint64(99), txNotFound.TransactionID)
}

func TestProcessRevertTransaction_AlreadyReverted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}
	txKey := domain.TransactionKey{Ledger: "test-ledger", ID: 3}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false).AnyTimes()
	mockStore.EXPECT().GetReverted(txKey).Return(true, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						TransactionId: 3,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var alreadyReverted *domain.ErrTransactionAlreadyReverted
	require.ErrorAs(t, err, &alreadyReverted)
	require.Equal(t, uint64(3), alreadyReverted.TransactionID)
}
