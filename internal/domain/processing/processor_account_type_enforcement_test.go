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

func TestProcessOrdersStrictTransactionAccountMetadataCapability(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		script       string
		addresses    []string
		mode         commonpb.ChartEnforcementMode
		wantAddress  string
		legacy       bool
		execute      bool
		metadataOnly bool
	}{
		{name: "historical transaction", addresses: []string{"rogue:1"}, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, legacy: true},
		{name: "historical ExecuteNumscript", script: `set_account_meta(@rogue:1, "k", "v")`, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, legacy: true, execute: true, metadataOnly: true},
		{name: "caller metadata", addresses: []string{"rogue:9", "rogue:1"}, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, wantAddress: "rogue:1"},
		{name: "first address wins across error types", addresses: []string{"zzz invalid", "rogue:1"}, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, wantAddress: "rogue:1"},
		{name: "Numscript metadata", script: `set_account_meta(@rogue:1, "k", "v")`, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, wantAddress: "rogue:1"},
		{name: "ExecuteNumscript metadata only", script: `set_account_meta(@rogue:1, "k", "v")`, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, wantAddress: "rogue:1", execute: true, metadataOnly: true},
		{name: "typed metadata", addresses: []string{"users:2", "world"}, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT},
		{name: "audit mode", addresses: []string{"rogue:1"}, mode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)
			info := strictLedgerInfoWithCompiledAccountType(t, processor, "test-ledger")
			info.DefaultEnforcementMode = test.mode
			scope := NewMockScope(controller)
			scope.EXPECT().GetClusterPolicy().Return(tightMetadataPolicy(1024)).AnyTimes()
			scope.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1234567890}).AsReader()).AnyTimes()
			scope.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
			scope.EXPECT().IncrementNextSequenceID().Return(uint64(1), nil).AnyTimes()
			setupLedgersStub(scope).expectGet(domain.LedgerKey{Name: "test-ledger"}, info.AsReader(), nil)
			setupBoundariesStub(scope).expectGet(domain.LedgerKey{Name: "test-ledger"}, (&raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}).AsReader(), nil)
			setupNumscriptVolumeMocks(scope)
			_, _ = stubsFor(scope).transactionStatesStubFor(scope)
			metadata, _ := stubsFor(scope).accountMetadataStubFor(scope)
			var written []string
			metadata.onPut(func(key domain.MetadataKey, _ *commonpb.MetadataValue) { written = append(written, key.Account) })
			payload := &servicepb.CreateTransactionPayload{Postings: []*commonpb.Posting{{Source: "world", Destination: "users:1", Amount: commonpb.NewUint256FromUint64(100), Asset: "USD/2"}}, AccountMetadata: map[string]*commonpb.MetadataMap{}}
			for _, address := range test.addresses {
				payload.AccountMetadata[address] = &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}}
			}
			if test.script != "" {
				payload.Postings = nil
				script := test.script
				if !test.metadataOnly {
					script += `
send [USD/2 100] (source = @world destination = @users:1)`
				}
				payload.Script = &commonpb.Script{Plain: script}
			}
			action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: payload}}
			if test.execute {
				action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_ExecuteNumscript{ExecuteNumscript: &servicepb.ExecuteNumscriptPayload{Script: payload.GetScript(), AccountMetadata: payload.GetAccountMetadata()}}}
			}
			request := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "test-ledger", Action: action}}}
			orders := []*raftcmdpb.Order{requestToOrder(t, request)}
			var result *OrdersResult
			var processErr domain.Describable
			if test.legacy {
				result, processErr = processor.ProcessOrders(orders, mockFactory(scope), noopSink{})
			} else {
				result, processErr = processor.ProcessOrdersWithStrictAccountMetadata(orders, mockFactory(scope), noopSink{})
			}
			if test.wantAddress == "" {
				require.Nil(t, processErr)
				require.NotNil(t, result)
				if test.legacy {
					require.Contains(t, written, "rogue:1")
				}

				return
			}
			var mismatch *domain.ErrAccountNotMatchingType
			require.ErrorAs(t, processErr, &mismatch)
			require.Equal(t, test.wantAddress, mismatch.Address)
			require.Empty(t, written)
		})
	}
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
