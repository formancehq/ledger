package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/proposals"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestProposalPayloadPolicyIncrementalRestore(t *testing.T) {
	t.Parallel()
	source, store, _ := newSigningParityMachine(t)
	policy := source.State.ClusterPolicy.CloneVT()
	policy.Revision = 2
	applyPolicy := func(index uint64, modern bool) {
		p := &raftcmdpb.Proposal{Id: index, Date: &commonpb.Timestamp{Data: 1700000000 + index},
			CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
			Orders:         []*raftcmdpb.Order{proposals.PolicyOrder(policy.CloneVT())},
		}
		wire := p
		if modern {
			wire.Orders = nil
			wire.Payload = &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: policy.CloneVT()}}
		}
		data, err := wire.MarshalVT()
		require.NoError(t, err)
		result, err := source.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
		require.True(t, result.Results[0].AuditEntryWritten)
	}
	applyPolicy(1, false)
	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), testLogger(), store, storage, "payload", "full")
	require.NoError(t, err)
	policy.Revision = 3
	policy.IdempotencyTtlMicros = 50000
	applyPolicy(2, false) // Historical entry after the checkpoint.
	policy.Revision = 4
	policy.QueryCheckpointLimit = 21
	applyPolicy(3, true)
	inc, err := RunIncrementalBackup(t.Context(), testLogger(), store, storage, "payload", 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, inc.LogEntriesExported)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey("payload"))
	require.NoError(t, err)
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	actual, err := query.ReadClusterPolicy(restored)
	require.NoError(t, err)
	require.True(t, proto.Equal(policy, actual), "delta must replace checkpoint policy")
	auditKeyCheckerClean(t, store)
	auditKeyCheckerClean(t, restored)
	recovered := recoveredAuditKeyMachine(t, restored)
	require.True(t, proto.Equal(policy, recovered.State.ClusterPolicy))
	// Recovery must preserve the policy and the next revision's conflict
	// check, rather than resetting policy to destination defaults.
	conflict := policy.CloneVT()
	conflict.QueryCheckpointLimit++
	p := &raftcmdpb.Proposal{Id: 10, Date: &commonpb.Timestamp{Data: 1700000010}, CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
		Payload: &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: conflict}},
	}
	data, err := p.MarshalVT()
	require.NoError(t, err)
	result, err := recovered.ApplyEntries(t.Context(), restored, &raftpb.Entry{Index: new(recovered.State.LastAppliedIndex + 1), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
	require.NoError(t, err)
	require.Error(t, result.Results[0].Error)
	require.False(t, result.Results[0].AuditEntryWritten)
	require.True(t, proto.Equal(policy, recovered.State.ClusterPolicy))
}
