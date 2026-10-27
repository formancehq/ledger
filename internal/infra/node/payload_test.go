package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestPayloadRotationAndCheckpointBoundaries(t *testing.T) {
	t.Parallel()
	config := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 2}}}},
	}}}
	entries := []state.DecodedEntry{
		{Entry: &raftpb.Entry{Index: new(uint64(1))}},
		{Entry: &raftpb.Entry{Index: new(uint64(2))}, Proposal: config},
		{Entry: &raftpb.Entry{Index: new(uint64(3))}},
	}
	require.Equal(t, 1, findApplyBoundaryDecoded(entries, 1000))
	require.Equal(t, 1, findApplyBoundaryDecoded(entries[1:], 1000))
	checkpoint := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}},
		}}}},
	}}}
	entries[1].Proposal = checkpoint
	require.Equal(t, 2, findApplyBoundaryDecoded(entries, 1000))
	require.ErrorContains(t, state.ValidateCheckpointEntryPositionsDecoded(entries), "applier must pre-split")
}
