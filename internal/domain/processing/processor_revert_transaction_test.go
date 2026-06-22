package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessRevertTransaction_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}

	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 3}

	// Source had balance: input=1000, output=0
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(0),
	}
	destVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetReverted(txKey).Return(false, nil)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()

	// Reversed posting: destination becomes source, source becomes destination
	// Original: bank -> users:123 for 100 USD
	// Revert:   users:123 -> bank for 100 USD
	mockStore.EXPECT().GetVolume(domain.NewVolumeKey("test-ledger", "users:123", "USD")).Return(sourceVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(domain.NewVolumeKey("test-ledger", "users:123", "USD"), gomock.Any())
	mockStore.EXPECT().GetVolume(domain.NewVolumeKey("test-ledger", "bank", "USD")).Return(destVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(domain.NewVolumeKey("test-ledger", "bank", "USD"), gomock.Any())

	mockStore.EXPECT().PutReverted(txKey, true)

	// Processor reads original transaction state, then updates it with RevertedByTransaction
	mockStore.EXPECT().GetTransactionState(txKey).Return(&commonpb.TransactionState{
		CreatedByLog: 42,
	}, nil)
	mockStore.EXPECT().PutTransactionState(txKey, gomock.Any())

	// Processor stores the new revert transaction state
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(50))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 5}, gomock.Any())
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

	// Without at_effective_date, the revert is stamped with the current FSM date.
	require.Equal(t, now, revertedTx.GetRevertTransaction().GetTimestamp())
}

func TestProcessRevertTransaction_AtEffectiveDate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 9_000_000_000}
	originalTimestamp := &commonpb.Timestamp{Data: 1_000_000_000}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}

	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 3}

	sourceVol := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1000), Output: commonpb.NewUint256FromUint64(0)}
	destVol := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100), Output: commonpb.NewUint256FromUint64(0)}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetReverted(txKey).Return(false, nil)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()

	mockStore.EXPECT().GetVolume(domain.NewVolumeKey("test-ledger", "users:123", "USD")).Return(sourceVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(domain.NewVolumeKey("test-ledger", "users:123", "USD"), gomock.Any())
	mockStore.EXPECT().GetVolume(domain.NewVolumeKey("test-ledger", "bank", "USD")).Return(destVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(domain.NewVolumeKey("test-ledger", "bank", "USD"), gomock.Any())

	mockStore.EXPECT().PutReverted(txKey, true)

	// Original transaction state carries the effective timestamp populated at create time.
	mockStore.EXPECT().GetTransactionState(txKey).Return(&commonpb.TransactionState{
		CreatedByLog: 42,
		Timestamp:    originalTimestamp,
	}, nil)
	mockStore.EXPECT().PutTransactionState(txKey, gomock.Any())

	mockStore.EXPECT().GetNextSequenceID().Return(uint64(50))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 5}, gomock.Any())
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						TransactionId:   3,
						AtEffectiveDate: true,
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

	revertTx := result.GetApply().GetLog().GetData().GetRevertedTransaction().GetRevertTransaction()
	require.NotNil(t, revertTx)
	require.Equal(t, originalTimestamp, revertTx.GetTimestamp(), "with at_effective_date the revert must inherit the original transaction's timestamp")
	require.NotEqual(t, now, revertTx.GetTimestamp(), "with at_effective_date the revert must NOT use the current FSM date")
	require.Equal(t, now, revertTx.GetInsertedAt(), "InsertedAt always reflects when the revert was applied")
}

func TestProcessRevertTransaction_AtEffectiveDate_MissingOriginalTimestamp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 9_000_000_000}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}

	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 3}

	sourceVol := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1000), Output: commonpb.NewUint256FromUint64(0)}
	destVol := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100), Output: commonpb.NewUint256FromUint64(0)}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetReverted(txKey).Return(false, nil)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()

	mockStore.EXPECT().GetVolume(domain.NewVolumeKey("test-ledger", "users:123", "USD")).Return(sourceVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(domain.NewVolumeKey("test-ledger", "users:123", "USD"), gomock.Any())
	mockStore.EXPECT().GetVolume(domain.NewVolumeKey("test-ledger", "bank", "USD")).Return(destVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(domain.NewVolumeKey("test-ledger", "bank", "USD"), gomock.Any())

	mockStore.EXPECT().PutReverted(txKey, true)

	// Simulate a TransactionState written before the Timestamp field existed (or
	// otherwise inconsistent state). With at_effective_date=true this must surface
	// loudly rather than silently falling back to s.GetDate().
	mockStore.EXPECT().GetTransactionState(txKey).Return(&commonpb.TransactionState{
		CreatedByLog: 42,
	}, nil)
	mockStore.EXPECT().PutTransactionState(txKey, gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						TransactionId:   3,
						AtEffectiveDate: true,
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
	require.Error(t, err)
	require.Nil(t, result)

	var inconsistent *domain.ErrTransactionStateInconsistent
	require.ErrorAs(t, err, &inconsistent)
	require.Equal(t, uint64(3), inconsistent.TransactionID)
	require.Equal(t, "revert at_effective_date", inconsistent.Operation)
}

func TestProcessRevertTransaction_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()

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

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}
	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 3}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
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
