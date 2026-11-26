package processing

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func metadataKeyPolicyChangeOrder(kind, key string) *raftcmdpb.Order {
	scoped := &raftcmdpb.LedgerScopedOrder{Ledger: "test-ledger"}
	apply := &raftcmdpb.LedgerApplyOrder{}
	switch kind {
	case "ledger":
		scoped.Payload = &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{DeleteLedgerMetadata: &raftcmdpb.DeleteLedgerMetadataOrder{Key: key}}
	case "set-field":
		apply.Data = &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{Key: key, TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Type: commonpb.MetadataType_METADATA_TYPE_INT64}}
	case "remove-field":
		apply.Data = &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{Key: key, TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT}}
	default:
		target := &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "users:alice"}}}
		if kind == "transaction" {
			target = &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 3}}
		}
		apply.Data = &raftcmdpb.LedgerApplyOrder_DeleteMetadata{DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{Key: key, Target: target}}
	}
	if kind != "ledger" {
		scoped.Payload = &raftcmdpb.LedgerScopedOrder_Apply{Apply: apply}
	}

	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: scoped}}
}

func TestProcessOrdersBareMetadataKeysPolicyChange(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"account", "transaction", "ledger", "set-field", "remove-field"} {
		for _, dimension := range []string{domain.MetadataLimitDimensionKey, domain.MetadataLimitDimensionCommand, "exact-boundary", "key-skippable-missing", "command-skippable-missing"} {
			if strings.HasSuffix(dimension, "-skippable-missing") && kind != "account" && kind != "transaction" {
				continue
			}
			t.Run(kind+"/"+dimension, func(t *testing.T) {
				t.Parallel()
				missing := strings.HasSuffix(dimension, "-skippable-missing")
				dimension := strings.TrimSuffix(dimension, "-skippable-missing")
				ctrl := gomock.NewController(t)
				scope := NewMockScope(ctrl)
				policy := tightMetadataPolicy(5)
				policy.MetadataMaxKeyBytes = 5
				policy.MetadataMaxValueBytes = 1
				policy.MetadataMaxCommandBytes = 10
				if dimension == domain.MetadataLimitDimensionKey {
					policy.MetadataMaxKeyBytes = 4
				}
				if dimension == domain.MetadataLimitDimensionCommand {
					policy.MetadataMaxCommandBytes = 9
				}
				require.NoError(t, domain.MetadataLimitsFromPolicy(policy).Validate())
				success := dimension == "exact-boundary"
				scope.EXPECT().GetClusterPolicy().Return(policy).AnyTimes()
				info := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1, MetadataSchema: &commonpb.MetadataSchema{AccountFields: map[string]*commonpb.MetadataFieldSchema{
					"abcde": {Type: commonpb.MetadataType_METADATA_TYPE_STRING}, "fghij": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
				}}}
				originalInfo := proto.Clone(info)
				ledgerStub, _ := stubsFor(scope).ledgersStubFor(scope)
				ledgerStub.onGet(func(domain.LedgerKey) (commonpb.LedgerInfoReader, error) { return info.AsReader(), nil })
				writes := 0
				recordWrite := func() { require.True(t, success, "rejected metadata must not mutate state"); writes++ }
				ledgerStub.onPut(func(_ domain.LedgerKey, value *commonpb.LedgerInfo) { recordWrite(); info = value })
				boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 10}
				boundaryStub := setupBoundariesStub(scope)
				boundaryStub.onGet(func(domain.LedgerKey) (raftcmdpb.LedgerBoundariesReader, error) { return boundaries.AsReader(), nil })
				boundaryStub.onPut(func(_ domain.LedgerKey, value *raftcmdpb.LedgerBoundaries) {
					require.True(t, success, "rejection must preserve boundaries")
					boundaries = value
				})
				stored := map[string]*commonpb.MetadataValue{"abcde": commonpb.NewStringValue("old"), "fghij": commonpb.NewStringValue("old")}
				if missing {
					stored = map[string]*commonpb.MetadataValue{}
				}
				transaction := &commonpb.TransactionState{Metadata: stored}
				originalTransaction := proto.Clone(transaction)
				switch kind {
				case "account":
					stub, _ := stubsFor(scope).accountMetadataStubFor(scope)
					stub.onGet(func(key domain.MetadataKey) (commonpb.MetadataValueReader, error) {
						value, ok := stored[key.Key]
						if !ok {
							return nil, domain.ErrNotFound
						}

						return value.AsReader(), nil
					})
					stub.onDelete(func(key domain.MetadataKey) { recordWrite(); delete(stored, key.Key) })
				case "transaction":
					stub, _ := stubsFor(scope).transactionStatesStubFor(scope)
					stub.onGet(func(domain.TransactionKey) (commonpb.TransactionStateReader, error) {
						return transaction.AsReader(), nil
					})
					stub.onPut(func(_ domain.TransactionKey, value *commonpb.TransactionState) { recordWrite(); transaction = value })
				case "ledger":
					stub := &kindStub[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{}
					scope.EXPECT().LedgerMetadata().Return(stub).AnyTimes()
					stub.onGet(func(key domain.LedgerMetadataKey) (commonpb.MetadataValueReader, error) {
						value, ok := stored[key.Key]
						if !ok {
							return nil, domain.ErrNotFound
						}

						return value.AsReader(), nil
					})
					stub.onDelete(func(key domain.LedgerMetadataKey) { recordWrite(); delete(stored, key.Key) })
				default:
					if success {
						expectGetIndex(scope, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
					}
				}
				if missing {
					// Skip-tolerant orders construct an overlay for every mutable accessor.
					stubsFor(scope).volumesStubFor(scope)
					stubsFor(scope).accountMetadataStubFor(scope)
					stubsFor(scope).transactionStatesStubFor(scope)
					stubsFor(scope).transactionReferencesStubFor(scope)
					stubsFor(scope).indexesStubFor(scope)
					setupPreparedQueriesStub(scope)
					scope.EXPECT().LedgerMetadata().Return(&kindStub[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{}).AnyTimes()
				}
				sink := NewMockSignalSink(ctrl)
				if success {
					scope.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 100}).AsReader()).AnyTimes()
					scope.EXPECT().IncrementNextSequenceID().Return(uint64(1), nil).Times(2)
					sink.EXPECT().Absorb(gomock.Any(), gomock.Any()).Times(2)
				}
				orders := []*raftcmdpb.Order{metadataKeyPolicyChangeOrder(kind, "abcde"), metadataKeyPolicyChangeOrder(kind, "fghij")}
				if missing {
					for _, order := range orders {
						order.GetLedgerScoped().GetApply().SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND}
					}
				}
				admissionLimits := domain.MetadataLimitsFromPolicy(tightMetadataPolicy(100))
				require.NoError(t, admissionLimits.Validate())
				require.Nil(t, domain.ValidateCommandMetadata(orders, admissionLimits))
				before := HashOrders(orders)
				processor, err := NewRequestProcessor(nil, 0)
				require.NoError(t, err)
				result, processErr := processor.ProcessOrders(orders, mockFactory(scope), sink)
				require.Equal(t, before, HashOrders(orders))
				if success {
					require.Nil(t, processErr)
					require.Len(t, result.CreatedLogs, 2)
					require.Equal(t, 2, writes)
					switch kind {
					case "account", "ledger":
						require.Empty(t, stored)
					case "transaction":
						require.Empty(t, transaction.GetMetadata())
					case "set-field":
						require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, info.GetMetadataSchema().GetAccountFields()["abcde"].GetType())
						require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, info.GetMetadataSchema().GetAccountFields()["fghij"].GetType())
					case "remove-field":
						require.Empty(t, info.GetMetadataSchema().GetAccountFields())
					}
				} else {
					require.Nil(t, result)
					var exceeded *domain.ErrMetadataLimitExceeded
					require.ErrorAs(t, processErr, &exceeded)
					require.Equal(t, dimension, exceeded.Dimension)
					if dimension == domain.MetadataLimitDimensionKey {
						require.Equal(t, uint64(5), exceeded.Actual)
						require.Equal(t, uint64(4), exceeded.Limit)
					} else {
						require.Equal(t, uint64(10), exceeded.Actual)
						require.Equal(t, uint64(9), exceeded.Limit)
					}
					require.Zero(t, writes)
					require.True(t, proto.Equal(originalInfo, info))
					require.True(t, proto.Equal(originalTransaction, transaction))
					if missing {
						require.Empty(t, stored)
					} else {
						require.Len(t, stored, 2)
					}
					require.Equal(t, uint64(5), boundaries.GetNextTransactionId())
					require.Equal(t, uint64(10), boundaries.GetNextLogId())
				}
			})
		}
	}
}
