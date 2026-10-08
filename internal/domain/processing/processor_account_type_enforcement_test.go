package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	internalstatepb "github.com/formancehq/ledger/v3/internal/proto/internalstatepb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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
		Input:  ledgerpb.NewUint256FromUint64(0),
		Output: ledgerpb.NewUint256FromUint64(0),
	}).AsReader()

	setupLedgersStub(mockStore).expectGet(domain.LedgerKey{Name: ledger}, ledgerInfo.AsReader(), nil)
	setupBoundariesStub(mockStore).expectGet(domain.LedgerKey{Name: ledger}, boundaries.AsReader(), nil)

	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(domain.NewVolumeKey(ledger, "world", "USD", ""), zeroVolume, nil)
	volumes.expectGet(domain.NewVolumeKey(ledger, "merchants:shop", "USD", ""), zeroVolume, nil)

	transactionStates := &kindStub[domain.TransactionKey, *internalstatepb.TransactionState, internalstatepb.TransactionStateReader]{}
	mockStore.EXPECT().TransactionStates().Return(transactionStates).AnyTimes()

	now := (&ledgerpb.Timestamp{Data: 1_234_567_890}).AsReader()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
						Postings: []*ledgerpb.Posting{{
							Source:      "world",
							Destination: "merchants:shop",
							Amount:      ledgerpb.NewUint256FromUint64(100),
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

	targetPostings := []*ledgerpb.Posting{{
		Source:      "world",
		Destination: "legacy:merchant",
		Amount:      ledgerpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}}

	transactionStates := &kindStub[domain.TransactionKey, *internalstatepb.TransactionState, internalstatepb.TransactionStateReader]{}
	transactionStates.expectGet(txKey, (&internalstatepb.TransactionState{
		CreatedByLog: 42,
		Postings:     targetPostings,
	}).AsReader(), nil)
	mockStore.EXPECT().TransactionStates().Return(transactionStates).AnyTimes()

	zeroVolume := (&raftcmdpb.VolumePair{
		Input:  ledgerpb.NewUint256FromUint64(0),
		Output: ledgerpb.NewUint256FromUint64(0),
	}).AsReader()
	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(domain.NewVolumeKey(ledger, "legacy:merchant", "USD", ""), zeroVolume, nil)
	volumes.expectGet(domain.NewVolumeKey(ledger, "world", "USD", ""), zeroVolume, nil)

	mockStore.EXPECT().GetReverted(txKey).Return(false, nil)
	mockStore.EXPECT().PutReverted(txKey, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return((&ledgerpb.Timestamp{Data: 1_234_567_890}).AsReader()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(50)).AnyTimes()

	order := &raftcmdpb.Order{
		// Admission binds what it observed of the target on every revert order;
		// apply rejects one that carries none.
		Technical: &raftcmdpb.OrderTechnical{
			RevertTargetDigest: domain.RevertTargetDigest(targetPostings, true),
		},
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
) *ledgerpb.LedgerInfo {
	t.Helper()

	info := &ledgerpb.LedgerInfo{
		Name:                   ledger,
		Id:                     1,
		DefaultEnforcementMode: ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
		AccountTypes: map[string]*ledgerpb.AccountType{
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
			},
		},
	}

	require.Len(t, compiledTypesFor(processor.compiledTypesCache, ledger, info.AsReader()), 1)

	return info
}
