package backup

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestCapabilityActivationDirectIncrementalRestore(t *testing.T) {
	t.Parallel()
	source, store, _ := newSigningParityMachine(t)
	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), testLogger(), store, storage, "direct", "full")
	require.NoError(t, err)
	for _, capabilityID := range []uint32{1, 2} {
		index := source.State.LastAppliedIndex + 1
		proposal := &raftcmdpb.Proposal{Id: index,
			CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
			Payload:        &raftcmdpb.Proposal_TransitionCapability{TransitionCapability: &commonpb.CapabilityActivation{Id: capabilityID, Active: true}},
		}
		data, err := proposal.MarshalVT()
		require.NoError(t, err)
		result, err := source.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
		incremental, err := RunIncrementalBackup(t.Context(), testLogger(), store, storage, "direct", 0)
		require.NoError(t, err)
		require.Zero(t, incremental.LogEntriesExported)
		manifest, err := ReadManifest(t.Context(), storage, ManifestKey("direct"))
		require.NoError(t, err)
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

func TestControlStateManifestRejectsLegacyDecoder(t *testing.T) {
	t.Parallel()
	current := &Manifest{ClusterPolicy: &ClusterPolicySnapshot{AppliedIndex: 42, Data: []byte("policy")}}
	data, err := json.Marshal(current)
	require.NoError(t, err)
	var legacy struct {
		ClusterPolicy *struct {
			AppliedIndex uint64 `json:"appliedIndex"`
			Data         []byte `json:"data"`
		} `json:"clusterPolicy"`
	}
	require.Error(t, json.Unmarshal(data, &legacy), "the released reader must fail rather than silently discard capability state")
	roundtrip, err := DecodeManifest(data)
	require.NoError(t, err)
	require.Equal(t, current.ClusterPolicy, roundtrip.ClusterPolicy)
	old, err := DecodeManifest([]byte(`{"clusterPolicy":{"appliedIndex":42,"data":"cG9saWN5"}}`))
	require.NoError(t, err)
	require.Equal(t, current.ClusterPolicy, old.ClusterPolicy)
	_, err = DecodeManifest([]byte(`{"clusterPolicy":{"data":{"version":99}}}`))
	require.ErrorContains(t, err, "unsupported control-state")
}

func TestCapabilityActivationPolicyOnlyIncrementalRestore(t *testing.T) {
	t.Parallel()
	source, store, _ := newSigningParityMachine(t)
	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), testLogger(), store, storage, "activation", "full")
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
	incremental, err := RunIncrementalBackup(t.Context(), testLogger(), store, storage, "activation", 0)
	require.NoError(t, err)
	require.Zero(t, incremental.LogEntriesExported)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey("activation"))
	require.NoError(t, err)
	require.NotNil(t, manifest.ClusterPolicy, "the post-checkpoint delta must carry the activation even without business logs")
	require.Equal(t, index, manifest.ClusterPolicy.AppliedIndex)
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	actual, err := query.ReadClusterPolicy(restored)
	require.NoError(t, err)
	require.True(t, proto.Equal(policy, actual))
	recovered := recoveredAuditKeyMachine(t, restored)
	require.True(t, proto.Equal(policy, recovered.State.ClusterPolicy))
	auditKeyCheckerClean(t, restored)
}
