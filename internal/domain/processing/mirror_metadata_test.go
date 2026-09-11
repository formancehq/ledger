package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func mirrorMetadataOrder(entry *raftcmdpb.MirrorLogEntry) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "mirrored", Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: entry}}}}}
}

func mirrorMetadataEntry(value string) *raftcmdpb.MirrorLogEntry {
	return &raftcmdpb.MirrorLogEntry{V2LogId: 1, Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{SavedMetadata: &raftcmdpb.MirrorSavedMetadata{Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "a"}}}, Metadata: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue(value)}}}}
}

func TestProcessOrdersMirrorMetadataRejectsBeforeMutation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, value, dimension string
		copies                 int
		unconfigured           bool
	}{
		{name: "value", value: "123456789", dimension: domain.MetadataLimitDimensionValue, copies: 1},
		{name: "aggregate", value: "123456", dimension: domain.MetadataLimitDimensionCommand, copies: 2},
		{name: "value shape", value: "x\x00y", copies: 1},
		{name: "unconfigured", value: "v", copies: 1, unconfigured: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			scope := NewMockScope(ctrl)
			policy := &commonpb.ClusterPolicy{Revision: 2, MetadataMaxEntriesPerEntity: 2, MetadataMaxKeyBytes: 4, MetadataMaxValueBytes: 8, MetadataMaxEntityBytes: 10, MetadataMaxCommandBytes: 12}
			if tc.unconfigured {
				policy = nil
			}
			scope.EXPECT().GetClusterPolicy().Return(policy)
			boundaries := &raftcmdpb.LedgerBoundaries{NextLogId: 1, NextTransactionId: 1}
			expectGetBoundaries(scope, domain.LedgerKey{Name: "mirrored"}, boundaries.AsReader(), nil)
			setupBoundariesStub(scope).onPut(func(domain.LedgerKey, *raftcmdpb.LedgerBoundaries) {
				t.Fatal("metadata rejection must precede boundary writes")
			})
			expectGetLedger(scope, domain.LedgerKey{Name: "mirrored"}, (&commonpb.LedgerInfo{Name: "mirrored", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}).AsReader(), nil)
			orders := make([]*raftcmdpb.Order, tc.copies)
			for i := range orders {
				entry := mirrorMetadataEntry(tc.value)
				entry.V2LogId = uint64(i + 1)
				orders[i] = mirrorMetadataOrder(entry)
			}
			before := HashOrders(orders)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)
			result, processErr := processor.ProcessOrders(orders, mockFactory(scope), NewMockSignalSink(ctrl))
			require.Nil(t, result)
			require.NotNil(t, processErr)
			if tc.unconfigured {
				require.ErrorIs(t, processErr, domain.ErrMetadataLimitsUnconfigured)
			}
			if tc.name == "value shape" {
				require.ErrorIs(t, processErr, domain.ErrMetadataValueContainsNullByte)
			}
			if tc.dimension != "" {
				var exceeded *domain.ErrMetadataLimitExceeded
				require.ErrorAs(t, processErr, &exceeded)
				require.Equal(t, tc.dimension, exceeded.Dimension)
			}
			require.Equal(t, before, HashOrders(orders))
			require.Zero(t, boundaries.GetLastMirrorV2LogId())
			require.Equal(t, uint64(1), boundaries.GetNextLogId())
		})
	}
}

func TestProcessOrdersMirrorMetadataExactCommandBoundary(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	scope := NewMockScope(ctrl)
	policy := tightMetadataPolicy(12)
	policy.MetadataMaxKeyBytes = 4
	scope.EXPECT().GetClusterPolicy().Return(policy).Times(2)
	scope.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 100}).AsReader()).AnyTimes()
	scope.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	scope.EXPECT().IncrementNextSequenceID().Return(uint64(1), nil).Times(2)
	boundaries := &raftcmdpb.LedgerBoundaries{NextLogId: 1, NextTransactionId: 1}
	stub := setupBoundariesStub(scope)
	stub.onGet(func(domain.LedgerKey) (raftcmdpb.LedgerBoundariesReader, error) { return boundaries.AsReader(), nil })
	stub.onPut(func(_ domain.LedgerKey, b *raftcmdpb.LedgerBoundaries) { boundaries = b })
	expectGetLedger(scope, domain.LedgerKey{Name: "mirrored"}, (&commonpb.LedgerInfo{Name: "mirrored", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}).AsReader(), nil).AnyTimes()
	metadataStub, _ := stubsFor(scope).accountMetadataStubFor(scope)
	writes := 0
	metadataStub.onPut(func(_ domain.MetadataKey, v *commonpb.MetadataValue) {
		require.Equal(t, "12345", v.GetStringValue())
		writes++
	})
	first, second := mirrorMetadataEntry("12345"), mirrorMetadataEntry("12345")
	second.V2LogId = 2
	orders := []*raftcmdpb.Order{mirrorMetadataOrder(first), mirrorMetadataOrder(second)}
	before := HashOrders(orders)
	sink := NewMockSignalSink(ctrl)
	sink.EXPECT().Absorb(gomock.Any(), gomock.Any()).Times(2)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)
	result, processErr := processor.ProcessOrders(orders, mockFactory(scope), sink)
	require.Nil(t, processErr)
	require.Len(t, result.CreatedLogs, 2)
	require.Equal(t, 2, writes)
	require.Equal(t, uint64(2), boundaries.GetLastMirrorV2LogId())
	require.Equal(t, before, HashOrders(orders))
}
