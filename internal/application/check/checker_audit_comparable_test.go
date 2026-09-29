package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAuditComparableLogIgnoresQueryCheckpointAppliedIndex(t *testing.T) {
	t.Parallel()

	stored := queryCheckpointLog(34)
	replayed := queryCheckpointLog(0)

	require.True(t, auditComparableLog(stored, 1).EqualVT(auditComparableLog(replayed, 1)))
}

func TestAuditReplayLogWithExecutionMetadataCarriesQueryCheckpointAppliedIndex(t *testing.T) {
	t.Parallel()

	stored := queryCheckpointLog(34)
	replayed := queryCheckpointLog(0)

	got := auditReplayLogWithExecutionMetadata(replayed, stored)
	require.Equal(t, uint64(34), got.GetPayload().GetCreatedQueryCheckpoint().GetAppliedIndex())
	require.Zero(t, replayed.GetPayload().GetCreatedQueryCheckpoint().GetAppliedIndex(), "the shared audit expectation must not be mutated")
}

func queryCheckpointLog(appliedIndex uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
				CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
					CheckpointId: 1,
					MaxSequence:  10,
					CreatedAt:    &commonpb.Timestamp{Data: 123},
					AppliedIndex: appliedIndex,
				},
			},
		},
	}
}
