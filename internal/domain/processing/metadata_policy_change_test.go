package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func metadataPolicyChangeOrder(kind string) *raftcmdpb.Order {
	metadata := map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("12345")}
	scoped := &raftcmdpb.LedgerScopedOrder{Ledger: "test-ledger"}
	switch kind {
	case "ledger":
		scoped.Payload = &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{Metadata: metadata}}
	case "revert":
		scoped.Payload = &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{RevertTransaction: &raftcmdpb.RevertTransactionOrder{TransactionId: 3, Metadata: metadata}}}}
	default:
		target := &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "users:alice"}}}
		if kind == "transaction" {
			target = &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 3}}
		}
		scoped.Payload = &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{AddMetadata: &raftcmdpb.SaveMetadataOrder{Target: target, Metadata: metadata}}}}
	}

	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: scoped}}
}

func TestProcessOrdersMetadataPolicyTightenedAfterAdmission(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"account", "transaction", "revert", "ledger"} {
		for _, dimension := range []string{domain.MetadataLimitDimensionValue, domain.MetadataLimitDimensionEntity, domain.MetadataLimitDimensionCommand} {
			t.Run(kind+"/"+dimension, func(t *testing.T) {
				t.Parallel()
				ctrl := gomock.NewController(t)
				scope := NewMockScope(ctrl)
				policy := tightMetadataPolicy(6)
				policy.MetadataMaxKeyBytes = 1
				policy.MetadataMaxValueBytes = 5
				policy.MetadataMaxCommandBytes = 100
				var actual, limit uint64
				switch dimension {
				case domain.MetadataLimitDimensionValue:
					policy.MetadataMaxValueBytes = 4
					actual, limit = 5, 4
				case domain.MetadataLimitDimensionEntity:
					policy.MetadataMaxEntityBytes = 5
					actual, limit = 6, 5
				case domain.MetadataLimitDimensionCommand:
					policy.MetadataMaxCommandBytes = 10
					actual, limit = 12, 10
				}
				require.NoError(t, domain.MetadataLimitsFromPolicy(policy).Validate())
				scope.EXPECT().GetClusterPolicy().Return(policy)
				ledgerKey := domain.LedgerKey{Name: "test-ledger"}
				expectGetLedger(scope, ledgerKey, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
				boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}
				if kind != "ledger" {
					expectGetBoundaries(scope, ledgerKey, boundaries.AsReader(), nil)
					setupBoundariesStub(scope).onPut(func(domain.LedgerKey, *raftcmdpb.LedgerBoundaries) {
						t.Fatal("rejected metadata must not change boundaries")
					})
				}
				if kind == "revert" {
					key := domain.TransactionKey{LedgerName: "test-ledger", ID: 3}
					scope.EXPECT().GetReverted(key).Return(false, nil)
					expectGetTransactionState(scope, key, (&commonpb.TransactionState{Postings: []*commonpb.Posting{{Source: "world", Destination: "users:alice", Asset: "USD", Amount: commonpb.NewUint256FromUint64(1)}}}).AsReader(), nil)
					stub, _ := stubsFor(scope).transactionStatesStubFor(scope)
					stub.onPut(func(domain.TransactionKey, *commonpb.TransactionState) {
						t.Fatal("rejected metadata must not change transaction state")
					})
				}
				orders := []*raftcmdpb.Order{metadataPolicyChangeOrder(kind)}
				if dimension == domain.MetadataLimitDimensionCommand {
					// Each map fits; only their combined command input exceeds the new policy.
					orders = append(orders, metadataPolicyChangeOrder(kind))
				}
				admissionLimits := domain.MetadataLimitsFromPolicy(tightMetadataPolicy(100))
				require.NoError(t, admissionLimits.Validate())
				require.Nil(t, domain.ValidateCommandMetadata(orders, admissionLimits))
				before := HashOrders(orders)
				processor, err := NewRequestProcessor(nil, 0)
				require.NoError(t, err)
				// No sink or mutation expectations: rejection must occur before either.
				result, processErr := processor.ProcessOrders(orders, mockFactory(scope), NewMockSignalSink(ctrl))
				require.Nil(t, result)
				var exceeded *domain.ErrMetadataLimitExceeded
				require.ErrorAs(t, processErr, &exceeded)
				require.Equal(t, dimension, exceeded.Dimension)
				require.Equal(t, actual, exceeded.Actual)
				require.Equal(t, limit, exceeded.Limit)
				require.Equal(t, before, HashOrders(orders))
				require.Equal(t, uint64(5), boundaries.GetNextTransactionId())
				require.Equal(t, uint64(10), boundaries.GetNextLogId())
			})
		}
	}
}

func TestProcessOrdersSavedMetadataExactCommandBoundary(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	scope := NewMockScope(ctrl)
	policy := tightMetadataPolicy(12)
	policy.MetadataMaxKeyBytes = 1
	require.NoError(t, domain.MetadataLimitsFromPolicy(policy).Validate())
	scope.EXPECT().GetClusterPolicy().Return(policy).Times(2)
	scope.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 100}).AsReader()).AnyTimes()
	scope.EXPECT().IncrementNextSequenceID().Return(uint64(1), nil).Times(2)
	boundaries := &raftcmdpb.LedgerBoundaries{NextLogId: 1, NextTransactionId: 1}
	stub := setupBoundariesStub(scope)
	stub.onGet(func(domain.LedgerKey) (raftcmdpb.LedgerBoundariesReader, error) { return boundaries.AsReader(), nil })
	stub.onPut(func(_ domain.LedgerKey, b *raftcmdpb.LedgerBoundaries) { boundaries = b })
	expectGetLedger(scope, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	metadataStub, _ := stubsFor(scope).accountMetadataStubFor(scope)
	writes := 0
	metadataStub.onPut(func(_ domain.MetadataKey, v *commonpb.MetadataValue) {
		require.Equal(t, "12345", v.GetStringValue())
		writes++
	})
	sink := NewMockSignalSink(ctrl)
	sink.EXPECT().Absorb(gomock.Any(), gomock.Any()).Times(2)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)
	orders := []*raftcmdpb.Order{metadataPolicyChangeOrder("account"), metadataPolicyChangeOrder("account")}
	before := HashOrders(orders)
	result, processErr := processor.ProcessOrders(orders, mockFactory(scope), sink)
	require.Nil(t, processErr)
	require.Len(t, result.CreatedLogs, 2)
	require.Equal(t, 2, writes)
	require.Equal(t, before, HashOrders(orders))
}
