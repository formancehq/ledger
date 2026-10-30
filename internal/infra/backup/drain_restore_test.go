package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

func applyDrainRestoreTechnicalEntry(t *testing.T, machine *state.Machine, store *dal.Store, index uint64, update *raftcmdpb.TechnicalUpdate) {
	t.Helper()
	proposal := &raftcmdpb.Proposal{
		Id:               index,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{update},
		CallerSnapshot:   commands.SystemCallerSnapshot(commands.ComponentClusterPeer),
	}
	data, err := proto.Marshal(proposal)
	require.NoError(t, err)
	result, err := machine.ApplyEntries(t.Context(), store, &raftpb.Entry{
		Index: new(index), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data,
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)
}

func TestDrainCheckpointIncrementalRestoreDiscardsSourceState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage := newInMemoryBackupStorage()
	source, sourceStore, _ := newSigningParityMachine(t)

	applySigningEntry(t, source, sourceStore, 1, signingRegisterOrder("before", make([]byte, 32), ""))
	applyDrainRestoreTechnicalEntry(t, source, sourceStore, 2, &raftcmdpb.TechnicalUpdate{
		Kind: &raftcmdpb.TechnicalUpdate_DrainMarker{DrainMarker: &raftcmdpb.DrainMarker{
			Epoch: "released-epoch", Members: []uint64{1, 2, 3},
		}},
	})
	applyDrainRestoreTechnicalEntry(t, source, sourceStore, 3, &raftcmdpb.TechnicalUpdate{
		Kind: &raftcmdpb.TechnicalUpdate_DrainRelease{DrainRelease: &raftcmdpb.DrainRelease{Epoch: "released-epoch"}},
	})
	applyDrainRestoreTechnicalEntry(t, source, sourceStore, 4, &raftcmdpb.TechnicalUpdate{
		Kind: &raftcmdpb.TechnicalUpdate_DrainMarker{DrainMarker: &raftcmdpb.DrainMarker{
			Epoch: "active-epoch", Members: []uint64{1, 2, 3},
		}},
	})
	checkpointDrain, err := query.ReadDrainState(sourceStore)
	require.NoError(t, err)
	require.Equal(t, "active-epoch", checkpointDrain.GetEpoch())
	require.Equal(t, []string{"released-epoch"}, checkpointDrain.GetPriorEpochs())
	require.NoError(t, sourceStore.Flush())
	_, err = RunBackup(ctx, testLogger(), sourceStore, storage, "drain-source", "drain-full")
	require.NoError(t, err)

	// Release on the source so a committed business write becomes a nonempty
	// post-checkpoint delta. The destination must not inherit either epoch.
	applyDrainRestoreTechnicalEntry(t, source, sourceStore, 5, &raftcmdpb.TechnicalUpdate{
		Kind: &raftcmdpb.TechnicalUpdate_DrainRelease{DrainRelease: &raftcmdpb.DrainRelease{Epoch: "active-epoch"}},
	})
	applySigningEntry(t, source, sourceStore, 6, signingRegisterOrder("after", make([]byte, 32), ""))
	require.NoError(t, sourceStore.Flush())
	inc, err := RunIncrementalBackup(ctx, testLogger(), sourceStore, storage, "drain-source", 0)
	require.NoError(t, err)
	require.Positive(t, inc.LogEntriesExported)
	require.Positive(t, inc.AuditEntriesExported)
	manifest, err := ReadManifest(ctx, storage, ManifestKey("drain-source"))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.NotEmpty(t, manifest.Exports)

	// Use the published checkpoint files and incremental export, not the live
	// source store. Restore finalization prepares this staged copy for a new
	// cluster identity before bootstrapping its FSM.
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	stagedDrain, err := query.ReadDrainState(restored)
	require.NoError(t, err)
	require.Equal(t, checkpointDrain, stagedDrain)
	require.NoError(t, attributes.PrepareForBackup(restored))
	stagedDrain, err = query.ReadDrainState(restored)
	require.NoError(t, err)
	require.Nil(t, stagedDrain)
	destinationConfig := &commonpb.PersistedConfig{NodeId: 1, ClusterId: "drain-destination", StorageSchemaVersion: 2}
	batch := restored.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneClusterPersistent, dal.SubGlobPersistedConfig}, destinationConfig))
	require.NoError(t, batch.Commit())
	storedConfig, err := query.ReadPersistedConfig(restored)
	require.NoError(t, err)
	require.Equal(t, "drain-destination", storedConfig.GetClusterId())

	for seq := uint64(1); seq <= 2; seq++ {
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(seq).Build()
		sourceBytes, err := dal.GetValue(sourceStore, key)
		require.NoError(t, err)
		restoredBytes, err := dal.GetValue(restored, key)
		require.NoError(t, err)
		require.Equal(t, sourceBytes, restoredBytes)
	}
	auditKeyCheckerClean(t, restored)

	destination := recoveredAuditKeyMachine(t, restored)
	epoch, cutoff := destination.Drain()
	require.Empty(t, epoch)
	require.Zero(t, cutoff)
	require.Zero(t, destination.DrainReleaseIndex())
	admitted := newScriptedNode(t, restored)
	auditBeforePolicy := lastAuditForKeyTest(t, restored).GetSequence()
	admitted.applyClusterPolicy(&commonpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	})
	// Native policy controls must not create an audit entry after restore.
	require.Equal(t, auditBeforePolicy, lastAuditForKeyTest(t, restored).GetSequence())
	admitted.apply(actions.CreateLedgerAction("new-on-destination", nil))
	created, err := query.GetLedgerByName(t.Context(), restored, "new-on-destination")
	require.NoError(t, err)
	require.Equal(t, "new-on-destination", created.GetName())
	require.Equal(t, auditBeforePolicy+1, lastAuditForKeyTest(t, restored).GetSequence())
	auditKeyCheckerClean(t, restored)
}
