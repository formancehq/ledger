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

	result, processErr := processor.ProcessOrder(requestToOrder(t, request), mockStore)
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

	targetPostings := []*commonpb.Posting{{
		Source:      "world",
		Destination: "legacy:merchant",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}}

	transactionStates := &kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]{}
	transactionStates.expectGet(txKey, (&commonpb.TransactionState{
		CreatedByLog: 42,
		Postings:     targetPostings,
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

	require.Len(t, compiledTypesFor(processor.compiledTypesCache, ledger, info.AsReader()), 1)

	return info
}

// TestProcessCreateTransactionAccountMetadataHonorsAccountTypes pins Heal
// defect s1-transaction-account-metadata-skips-strict-1. A transaction used to
// type-check only its postings, so the account metadata it carried — from the
// caller or from Numscript set_account_meta — reached an address that a direct
// SaveMetadata on the same STRICT ledger refuses with ACCOUNT_NOT_MATCHING_TYPE.
func TestProcessCreateTransactionAccountMetadataHonorsAccountTypes(t *testing.T) {
	t.Parallel()

	const ledger = "test-ledger"

	callerMetadata := func(addresses ...string) map[string]*commonpb.MetadataMap {
		out := make(map[string]*commonpb.MetadataMap, len(addresses))
		for _, address := range addresses {
			out[address] = &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}}
		}

		return out
	}

	testCases := []struct {
		name            string
		mode            commonpb.ChartEnforcementMode
		accountMetadata map[string]*commonpb.MetadataMap
		script          string
		// rejected is the address reported by ACCOUNT_NOT_MATCHING_TYPE; empty
		// means the transaction is accepted.
		rejected string
	}{
		{
			name:            "strict rejects caller metadata on an untyped account",
			mode:            commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
			accountMetadata: callerMetadata("rogue:1"),
			rejected:        "rogue:1",
		},
		{
			name:            "strict reports the smallest untyped address",
			mode:            commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
			accountMetadata: callerMetadata("users:2", "rogue:9", "rogue:1", "rogue:5"),
			rejected:        "rogue:1",
		},
		{
			name:     "strict rejects Numscript metadata on an untyped account",
			mode:     commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
			script:   `set_account_meta(@rogue:1, "k", "v")`,
			rejected: "rogue:1",
		},
		{
			name:            "strict accepts metadata on typed accounts and world",
			mode:            commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
			accountMetadata: callerMetadata("users:2", "world"),
		},
		{
			name:            "audit accepts metadata on an untyped account",
			mode:            commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			accountMetadata: callerMetadata("rogue:1"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			ledgerInfo := strictLedgerInfoWithCompiledAccountType(t, processor, ledger)
			ledgerInfo.DefaultEnforcementMode = tc.mode

			scope := NewMockScope(ctrl)
			scope.EXPECT().GetClusterPolicy().Return(tightMetadataPolicy(1024)).AnyTimes()
			scope.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1_234_567_890}).AsReader()).AnyTimes()
			scope.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
			setupLedgersStub(scope).expectGet(domain.LedgerKey{Name: ledger}, ledgerInfo.AsReader(), nil)
			setupBoundariesStub(scope).expectGet(domain.LedgerKey{Name: ledger},
				(&raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}).AsReader(), nil)
			setupNumscriptVolumeMocks(scope)
			_, _ = stubsFor(scope).transactionStatesStubFor(scope)
			accountMetadata, _ := stubsFor(scope).accountMetadataStubFor(scope)

			var written []string
			accountMetadata.onPut(func(key domain.MetadataKey, _ *commonpb.MetadataValue) {
				written = append(written, key.Account)
			})

			payload := &servicepb.CreateTransactionPayload{AccountMetadata: tc.accountMetadata}
			if tc.script != "" {
				payload.Script = &commonpb.Script{Plain: tc.script + `
send [USD/2 100] (source = @world destination = @users:1)
`}
			} else {
				payload.Postings = []*commonpb.Posting{{
					Source:      "world",
					Destination: "users:1",
					Amount:      commonpb.NewUint256FromUint64(100),
					Asset:       "USD/2",
				}}
			}

			request := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: payload}},
			}}}

			result, processErr := processor.ProcessOrder(requestToOrder(t, request), scope)

			if tc.rejected == "" {
				require.Nil(t, processErr)
				require.NotNil(t, result)

				return
			}

			require.Nil(t, result)

			var notMatching *domain.ErrAccountNotMatchingType
			require.ErrorAs(t, processErr, &notMatching)
			require.Equal(t, tc.rejected, notMatching.Address)
			require.Empty(t, written, "a rejected transaction must not stage account metadata")
		})
	}
}
