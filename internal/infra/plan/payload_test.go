package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestRunnerSerializesPayloadOnEveryRetry(t *testing.T) {
	t.Parallel()
	for _, policy := range []bool{false, true} {
		t.Run(map[bool]string{false: "technical", true: "policy"}[policy], func(t *testing.T) {
			t.Parallel()
			tracker := node.NewIndexTracker(1)
			builder := newSubmitTestBuilder(t, tracker)
			proposer := &recordingProposer{tracker: tracker, staleRemain: 1}
			cmd := commands.NewTechnicalCommand(&raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: make([]byte, 32)}})
			cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
			ops := []WriteOperation{{Target: &cmd.GetBatch().TechnicalUpdates[0].CoverageBits}}
			if policy {
				cmd.Payload = &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: &commonpb.ClusterPolicy{Revision: 2}}}
				cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPolicy)
				ops = nil
			}
			require.NoError(t, SubmitTechnical(t.Context(), builder, proposer, cmd, ops, "payload"))
			all := proposer.capturedProposals(t)
			require.Len(t, all, 2)
			for _, p := range all {
				require.NotNil(t, p.GetPayload())
				require.Empty(t, p.GetOrders())
				require.Empty(t, p.GetTechnicalUpdates())
				require.NotZero(t, p.GetPredictedIndex())
				if policy {
					require.NotNil(t, p.GetSetClusterPolicy())
					require.Equal(t, uint64(2), p.GetSetClusterPolicy().GetPolicy().GetRevision())
				} else {
					require.Len(t, p.GetBatch().GetTechnicalUpdates(), 1)
					require.Len(t, p.GetBatch().GetTechnicalUpdates()[0].GetAuditKey(), 32)
				}
			}
			require.NotEqual(t, all[0].GetId(), all[1].GetId())
			require.Greater(t, all[1].GetPredictedIndex(), all[0].GetPredictedIndex())
			require.NotNil(t, cmd.GetPayload(), "serialization must leave the proposal intact")
			require.Empty(t, cmd.GetOrders())
			require.Empty(t, cmd.GetTechnicalUpdates())
		})
	}
}
