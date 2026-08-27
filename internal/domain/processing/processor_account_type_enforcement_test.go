package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestProcessCreateTransactionRejectsAccountOutsideConfiguredTypes(t *testing.T) {
	t.Parallel()

	const ledger = "test-ledger"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := strictLedgerInfoWithCompiledAccountType(t, processor, ledger)
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	zeroVolume := (&raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}).AsReader()

	setupLedgersStub(mockStore).expectGet(domain.LedgerKey{Name: ledger}, ledgerInfo.AsReader(), nil)
	setupBoundariesStub(mockStore).expectGet(domain.LedgerKey{Name: ledger}, boundaries.AsReader(), nil)

	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(domain.NewVolumeKey(ledger, "world", "USD", ""), zeroVolume, nil)
	volumes.expectGet(domain.NewVolumeKey(ledger, "merchants:shop", "USD", ""), zeroVolume, nil)

	transactionStates := &kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]{}
	mockStore.EXPECT().TransactionStates().Return(transactionStates).AnyTimes()

	now := (&commonpb.Timestamp{Data: 1_234_567_890}).AsReader()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false).AnyTimes()

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{{
							Source:      "world",
							Destination: "merchants:shop",
							Amount:      commonpb.NewUint256FromUint64(100),
							Asset:       "USD",
						}},
					},
				}},
			},
		},
	}

	result, processErr := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Nil(t, result)

	var notMatching *domain.ErrAccountNotMatchingType
	require.ErrorAs(t, processErr, &notMatching)
	require.Equal(t, "merchants:shop", notMatching.Address)
}

func TestProcessRevertTransactionRejectsAccountOutsideConfiguredTypes(t *testing.T) {
	t.Parallel()

	const ledger = "test-ledger"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := strictLedgerInfoWithCompiledAccountType(t, processor, ledger)
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}
	txKey := domain.TransactionKey{LedgerName: ledger, ID: 3}

	setupLedgersStub(mockStore).expectGet(domain.LedgerKey{Name: ledger}, ledgerInfo.AsReader(), nil)
	setupBoundariesStub(mockStore).expectGet(domain.LedgerKey{Name: ledger}, boundaries.AsReader(), nil)

	transactionStates := &kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]{}
	transactionStates.expectGet(txKey, (&commonpb.TransactionState{
		CreatedByLog: 42,
		Postings: []*commonpb.Posting{{
			Source:      "world",
			Destination: "legacy:merchant",
			Amount:      commonpb.NewUint256FromUint64(100),
			Asset:       "USD",
		}},
	}).AsReader(), nil)
	mockStore.EXPECT().TransactionStates().Return(transactionStates).AnyTimes()

	zeroVolume := (&raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}).AsReader()
	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(domain.NewVolumeKey(ledger, "legacy:merchant", "USD", ""), zeroVolume, nil)
	volumes.expectGet(domain.NewVolumeKey(ledger, "world", "USD", ""), zeroVolume, nil)

	mockStore.EXPECT().GetReverted(txKey).Return(false, nil)
	mockStore.EXPECT().PutReverted(txKey, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1_234_567_890}).AsReader()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(50)).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
						RevertTransaction: &raftcmdpb.RevertTransactionOrder{
							TransactionId: 3,
							Force:         true,
						},
					}},
				},
			},
		},
	}

	result, processErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, result)

	var notMatching *domain.ErrAccountNotMatchingType
	require.ErrorAs(t, processErr, &notMatching)
	require.Equal(t, "legacy:merchant", notMatching.Address)
}

func strictLedgerInfoWithCompiledAccountType(
	t *testing.T,
	processor *RequestProcessor,
	ledger string,
) *commonpb.LedgerInfo {
	t.Helper()

	info := &commonpb.LedgerInfo{
		Name:                   ledger,
		Id:                     1,
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
		AccountTypes: map[string]*commonpb.AccountType{
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
			},
		},
	}

	require.Len(t, compiledTypesFor(processor.compiledTypesCache, ledger, info), 1)

	return info
}
