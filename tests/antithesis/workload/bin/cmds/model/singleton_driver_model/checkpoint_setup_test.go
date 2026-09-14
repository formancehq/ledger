package main

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
)

func TestCheckpointBaselineIncludesDeletedLastAllocation(t *testing.T) {
	t.Parallel()
	listed := &clusterpb.ListQueryCheckpointsResponse{Checkpoints: []*clusterpb.QueryCheckpointInfo{{CheckpointId: 3}}}
	var reads []uint64
	ids, next, err := checkpointBaseline(listed, 10, func(sequence uint64) (*commonpb.Log, error) {
		reads = append(reads, sequence)
		entry := &commonpb.Log{Sequence: sequence}
		if sequence == 9 {
			entry.Payload = &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{CheckpointId: 7}}}
		}
		if sequence == 8 {
			entry.Payload = &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: 7}}}
		}
		return entry, nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 9, 8}, reads)
	require.Equal(t, []uint64{3}, ids)
	require.Equal(t, uint64(8), next)
}

func TestCheckpointBaselineFreshAndInvalidHistory(t *testing.T) {
	t.Parallel()
	ids, next, err := checkpointBaseline(&clusterpb.ListQueryCheckpointsResponse{}, 0, func(uint64) (*commonpb.Log, error) {
		t.Fatal("empty history must not read a log")
		return nil, nil
	})
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Equal(t, uint64(1), next)
	failure := errors.New("read failed")
	_, _, err = checkpointBaseline(nil, 1, func(uint64) (*commonpb.Log, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
	_, _, err = checkpointBaseline(nil, 1, func(uint64) (*commonpb.Log, error) { return &commonpb.Log{Sequence: 2}, nil })
	require.ErrorContains(t, err, "wrong sequence")
	_, _, err = checkpointBaseline(&clusterpb.ListQueryCheckpointsResponse{Checkpoints: []*clusterpb.QueryCheckpointInfo{{CheckpointId: 1}}}, 0, nil)
	require.ErrorContains(t, err, "invalid checkpoint baseline registry")
}
