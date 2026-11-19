package backup

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestReadIndexBackupPreservesCapabilityControlState(t *testing.T) {
	t.Parallel()
	source, store, _ := newSigningParityMachine(t)
	index := source.State.LastAppliedIndex + 1
	proposal := &raftcmdpb.Proposal{
		Id:             index,
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
		Payload:        &raftcmdpb.Proposal_CapabilityActivation{CapabilityActivation: &commonpb.CapabilityActivation{Id: 1, Active: true}},
	}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	result, err := source.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	readIndex, err := readstore.New(t.TempDir(), testLogger(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readIndex.Close()) })
	seed := filepath.Join(t.TempDir(), "seed")
	require.NoError(t, readIndex.CreateCheckpoint(seed))
	storage := newInMemoryBackupStorage()
	_, err = RunBackupWithReadIndex(t.Context(), testLogger(), store, storage, "control", "full", seed)
	require.NoError(t, err)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey("control"))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint.ReadIndex)
	require.NotEmpty(t, manifest.Checkpoint.ReadIndex.Files)
	require.NotNil(t, manifest.ClusterPolicy, "publishing the read index must retain the control-state override")
	require.Equal(t, index, manifest.ClusterPolicy.AppliedIndex)
	var policy commonpb.ClusterPolicy
	require.NoError(t, policy.UnmarshalVT(manifest.ClusterPolicy.Data))
	require.True(t, proto.Equal(source.State.ClusterPolicy, &policy))
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	actual, err := query.ReadClusterPolicy(restored)
	require.NoError(t, err)
	require.True(t, proto.Equal(source.State.ClusterPolicy, actual))
	auditKeyCheckerClean(t, restored)
}
