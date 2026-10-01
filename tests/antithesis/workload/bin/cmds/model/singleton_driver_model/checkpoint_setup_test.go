package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestCheckpointBaselineUsesProbeAllocationFrontier(t *testing.T) {
	t.Parallel()
	listed := &commonpb.ListQueryCheckpointsResponse{Checkpoints: []*commonpb.QueryCheckpointInfo{{CheckpointId: 3}, {CheckpointId: 7}}}
	ids, next, err := checkpointBaseline(listed, 12)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 7, 12}, ids)
	require.Equal(t, uint64(13), next)
}

func TestCheckpointBaselineRejectsInvalidRegistry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		listed  *commonpb.ListQueryCheckpointsResponse
		probeID uint64
	}{
		{name: "zero probe", probeID: 0},
		{name: "maximum probe", probeID: ^uint64(0)},
		{name: "zero registry ID", listed: checkpointRegistry(0), probeID: 4},
		{name: "registry reaches probe", listed: checkpointRegistry(4), probeID: 4},
		{name: "duplicate registry ID", listed: checkpointRegistry(2, 2), probeID: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := checkpointBaseline(tc.listed, tc.probeID)
			require.Error(t, err)
		})
	}
}

func checkpointRegistry(ids ...uint64) *commonpb.ListQueryCheckpointsResponse {
	response := &commonpb.ListQueryCheckpointsResponse{}
	for _, id := range ids {
		response.Checkpoints = append(response.Checkpoints, &commonpb.QueryCheckpointInfo{CheckpointId: id})
	}

	return response
}
