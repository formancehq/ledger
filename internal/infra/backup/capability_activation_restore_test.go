package backup

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestCapabilityActivationDirectCheckpointRestore(t *testing.T) {
	t.Parallel()
	source, store, _ := newCreationTestMachine(t)
	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), logging.Testing(), store, storage, "direct", "first")
	require.NoError(t, err)
	for _, capabilityID := range []uint32{1, 2} {
		index := source.State.LastAppliedIndex + 1
		proposal := &raftcmdpb.Proposal{Id: index,
			CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
			Payload:        &raftcmdpb.Proposal_CapabilityActivation{CapabilityActivation: &commonpb.CapabilityActivation{Id: capabilityID, Active: true}},
		}
		data, err := proposal.MarshalVT()
		require.NoError(t, err)
		result, err := source.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
		_, err = RunBackup(t.Context(), logging.Testing(), store, storage, "direct", fmt.Sprintf("next-%d", capabilityID))
		require.NoError(t, err)
		manifest, err := ReadManifest(t.Context(), storage, ManifestKey("direct"))
		require.NoError(t, err)
		require.Empty(t, manifest.Exports)
		restored := restoreAuditKeyCheckpoint(t, storage, manifest)
		policy, err := query.ReadClusterPolicy(restored)
		require.NoError(t, err)
		require.True(t, proto.Equal(source.State.ClusterPolicy, policy))
		require.Len(t, policy.GetCapabilityActivations(), int(capabilityID))
		for _, activation := range policy.GetCapabilityActivations() {
			require.True(t, activation.GetActive())
		}
		auditKeyCheckerClean(t, restored)
	}
}

func TestCapabilityActivationPolicyOnlyCheckpointRestore(t *testing.T) {
	t.Parallel()
	source, store, _ := newCreationTestMachine(t)
	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), logging.Testing(), store, storage, "activation", "first")
	require.NoError(t, err)
	policy := source.State.ClusterPolicy.CloneVT()
	policy.Revision++
	policy.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: 1, Active: true}}
	index := source.State.LastAppliedIndex + 1
	proposal := &raftcmdpb.Proposal{Id: index, Date: &commonpb.Timestamp{Data: 1700000000 + index},
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
		Payload:        &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: policy}},
	}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	result, err := source.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.False(t, result.Results[0].AuditEntryWritten)
	_, err = RunBackup(t.Context(), logging.Testing(), store, storage, "activation", "second")
	require.NoError(t, err)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey("activation"))
	require.NoError(t, err)
	require.Empty(t, manifest.Exports)
	require.Nil(t, manifest.ClusterPolicy)
	require.Equal(t, index, manifest.Checkpoint.LastAppliedIndex)
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	actual, err := query.ReadClusterPolicy(restored)
	require.NoError(t, err)
	require.True(t, proto.Equal(policy, actual))
	recovered := recoveredAuditKeyMachine(t, restored)
	require.True(t, proto.Equal(policy, recovered.State.ClusterPolicy))
	auditKeyCheckerClean(t, restored)
}
