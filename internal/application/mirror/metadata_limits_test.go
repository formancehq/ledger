package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func mirrorMetadataPolicy() *commonpb.ClusterPolicy {
	return &commonpb.ClusterPolicy{Revision: 1, QueryCheckpointLimit: 1, MetadataMaxEntriesPerEntity: 2, MetadataMaxKeyBytes: 4, MetadataMaxValueBytes: 8, MetadataMaxEntityBytes: 10, MetadataMaxCommandBytes: 12}
}

func writeMirrorMetadataPolicy(t *testing.T, store *dal.Store, policy *commonpb.ClusterPolicy) {
	t.Helper()
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, policy))
	require.NoError(t, batch.Commit())
}

func TestWorkerMetadataLimitsRejectBeforeProposal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data, logType, dimension string
		copies                         int
		unconfigured                   bool
	}{
		{name: "value", logType: "SET_METADATA", data: `{"targetType":"ACCOUNT","targetId":"a","metadata":{"k":"123456789"}}`, dimension: domain.MetadataLimitDimensionValue, copies: 1},
		{name: "command", logType: "SET_METADATA", data: `{"targetType":"ACCOUNT","targetId":"a","metadata":{"k":"123456"}}`, dimension: domain.MetadataLimitDimensionCommand, copies: 2},
		{name: "delete key", logType: "DELETE_METADATA", data: `{"targetType":"ACCOUNT","targetId":"a","key":"12345"}`, dimension: domain.MetadataLimitDimensionKey, copies: 1},
		{name: "empty delete key", logType: "DELETE_METADATA", data: `{"targetType":"ACCOUNT","targetId":"a","key":""}`, copies: 1},
		{name: "unconfigured", logType: "SET_METADATA", data: `{"targetType":"ACCOUNT","targetId":"a","metadata":{"k":"v"}}`, copies: 1, unconfigured: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			builder, store := newTestBuilder(t)
			if !tc.unconfigured {
				writeMirrorMetadataPolicy(t, store, mirrorMetadataPolicy())
			}
			logs := make([]v2.V2Log, tc.copies)
			for i := range logs {
				logs[i] = v2.V2Log{ID: uint64(i + 1), Type: tc.logType, Date: "2023-11-14T22:13:20Z", Data: []byte(tc.data)}
			}
			source := v2.NewMockSource(gomock.NewController(t))
			source.EXPECT().FetchLogs(gomock.Any(), uint64(0), gomock.Any()).Return(logs, false, nil).Times(2)
			proposer := &stubProposer{outcome: proposeApplied}
			w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)
			for range 2 {
				_, err := w.processBatch(context.Background())
				var business *domain.BusinessError
				require.ErrorAs(t, err, &business)
				if tc.name == "empty delete key" {
					require.ErrorIs(t, err, domain.ErrMetadataKeyEmpty)
				}
				if tc.unconfigured {
					require.ErrorIs(t, err, domain.ErrMetadataLimitsUnconfigured)
				}
				if tc.dimension != "" {
					var exceeded *domain.ErrMetadataLimitExceeded
					require.ErrorAs(t, err, &exceeded)
					require.Equal(t, tc.dimension, exceeded.Dimension)
				}
				require.Empty(t, proposer.recorded())
				require.Zero(t, w.lastAppliedV2LogID)
				require.Equal(t, uint64(1), w.nextTxID)
			}
		})
	}
}

func TestWorkerMetadataPolicyIncreaseRetriesSameSourceBatch(t *testing.T) {
	t.Parallel()
	builder, store := newTestBuilder(t)
	policy := mirrorMetadataPolicy()
	writeMirrorMetadataPolicy(t, store, policy)
	logs := []v2.V2Log{{ID: 1, Type: "SET_METADATA", Date: "2023-11-14T22:13:20Z", Data: []byte(`{"targetType":"ACCOUNT","targetId":"a","metadata":{"k":"123456789"}}`)}}
	source := v2.NewMockSource(gomock.NewController(t))
	source.EXPECT().FetchLogs(gomock.Any(), uint64(0), gomock.Any()).Return(logs, false, nil).Times(2)
	proposer := &stubProposer{outcome: proposeApplied}
	w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)
	_, err := w.processBatch(context.Background())
	var exceeded *domain.ErrMetadataLimitExceeded
	require.ErrorAs(t, err, &exceeded)
	require.Equal(t, domain.MetadataLimitDimensionValue, exceeded.Dimension)
	require.Empty(t, proposer.recorded())
	require.Zero(t, w.lastAppliedV2LogID)
	policy.Revision++
	policy.MetadataMaxValueBytes = 9
	writeMirrorMetadataPolicy(t, store, policy)
	_, err = w.processBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, proposer.recorded(), 1)
	require.Equal(t, uint64(1), w.lastAppliedV2LogID)
	require.Equal(t, "123456789", proposer.recorded()[0].cmd.GetOrders()[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetSavedMetadata().GetMetadata()["k"].GetStringValue())
}
