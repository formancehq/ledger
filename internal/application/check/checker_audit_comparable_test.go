package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
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

func queryCheckpointLog(appliedIndex uint64) *ledgerpb.Log {
	return &ledgerpb.Log{
		Sequence: 1,
		Payload: &ledgerpb.LogPayload{
			Type: &ledgerpb.LogPayload_CreatedQueryCheckpoint{
				CreatedQueryCheckpoint: &ledgerpb.CreatedQueryCheckpointLog{
					CheckpointId: 1,
					MaxSequence:  10,
					CreatedAt:    &ledgerpb.Timestamp{Data: 123},
					AppliedIndex: appliedIndex,
				},
			},
		},
	}
}
