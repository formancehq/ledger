package backup

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestProposalPayloadPolicyIncrementalRestore(t *testing.T) {
	t.Parallel()
	for _, legacyDelta := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_delta_%t", legacyDelta), func(t *testing.T) {
			t.Parallel()
			source, store, _ := newSigningParityMachine(t)
			policy := source.State.ClusterPolicy.CloneVT()
			policy.Revision = 2
			applyPolicy := func(index uint64, modern bool) {
				p := &raftcmdpb.Proposal{Id: index, Date: &commonpb.Timestamp{Data: 1700000000 + index},
					CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
					Orders:         []*raftcmdpb.Order{{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: policy.CloneVT()}}}}}},
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
				require.Equal(t, !modern, result.Results[0].AuditEntryWritten)
			}
			applyPolicy(1, false)
			storage := newInMemoryBackupStorage()
			_, err := RunBackup(t.Context(), testLogger(), store, storage, "payload", "full")
			require.NoError(t, err)
			policy.Revision = 3
			policy.IdempotencyTtlMicros = 50000
			if legacyDelta {
				applyPolicy(2, false)
			} // Historical entry after the checkpoint.
			policy.Revision = 4
			policy.QueryCheckpointLimit = 21
			applyPolicy(source.State.LastAppliedIndex+1, true)
			applyPolicy(source.State.LastAppliedIndex+1, true) // Native no-op consumes neither audit nor log sequence.
			inc, err := RunIncrementalBackup(t.Context(), testLogger(), store, storage, "payload", 0)
			require.NoError(t, err)
			if legacyDelta {
				require.EqualValues(t, 1, inc.LogEntriesExported)
			} else {
				require.Zero(t, inc.LogEntriesExported)
			}
			manifest, err := ReadManifest(t.Context(), storage, ManifestKey("payload"))
			require.NoError(t, err)
			require.NotNil(t, manifest.ClusterPolicy)
			require.Equal(t, source.State.LastAppliedIndex, manifest.ClusterPolicy.AppliedIndex)
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
		})
	}
}

func TestPolicyOnlyBackupPublicationAndRetry(t *testing.T) {
	t.Parallel()
	source, store, _ := newSigningParityMachine(t)
	policy := source.State.ClusterPolicy.CloneVT()
	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), testLogger(), store, storage, "policy", "full")
	require.NoError(t, err)
	old, err := ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)
	policy.Revision++
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, policy))
	require.NoError(t, state.SetAppliedIndex(batch, 10))
	require.NoError(t, batch.Commit())

	mock := NewMockStorage(gomock.NewController(t))
	mock.EXPECT().GetFile(gomock.Any(), gomock.Any()).DoAndReturn(storage.GetFile).AnyTimes()
	mock.EXPECT().ListFiles(gomock.Any(), gomock.Any()).DoAndReturn(storage.ListFiles).AnyTimes()
	gomock.InOrder(
		mock.EXPECT().PutFile(gomock.Any(), ManifestKey("policy"), gomock.Any(), gomock.Any()).Return(errors.New("manifest unavailable")),
		mock.EXPECT().PutFile(gomock.Any(), ManifestKey("policy"), gomock.Any(), gomock.Any()).DoAndReturn(storage.PutFile),
		mock.EXPECT().PutFile(gomock.Any(), ManifestKey("policy"), gomock.Any(), gomock.Any()).DoAndReturn(storage.PutFile),
	)
	_, err = RunIncrementalBackup(t.Context(), testLogger(), store, mock, "policy", 0)
	require.ErrorContains(t, err, "manifest unavailable")
	retained, err := ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)
	require.Equal(t, old, retained)
	retry, err := RunIncrementalBackup(t.Context(), testLogger(), store, mock, "policy", 0)
	require.NoError(t, err)
	require.Zero(t, retry.LogEntriesExported)
	published, err := ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)
	require.Equal(t, policy.MarshalDeterministicVT(nil), published.ClusterPolicy.Data)
	require.Equal(t, uint64(10), published.ClusterPolicy.AppliedIndex)
	require.Empty(t, published.Exports)

	// A newer observed snapshot advances the publication watermark even if
	// no-op policy controls or unrelated technical work leave policy identical.
	batch = store.OpenWriteSession()
	require.NoError(t, state.SetAppliedIndex(batch, 20))
	require.NoError(t, batch.Commit())
	_, err = RunIncrementalBackup(t.Context(), testLogger(), store, mock, "policy", 0)
	require.NoError(t, err)
	latest, err := ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)
	require.Equal(t, published.ClusterPolicy.Data, latest.ClusterPolicy.Data)
	require.Equal(t, uint64(20), latest.ClusterPolicy.AppliedIndex)
	published = latest

	// A snapshot rollback must not overwrite the newer published policy.
	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, source.State.ClusterPolicy))
	require.NoError(t, state.SetAppliedIndex(batch, 9))
	require.NoError(t, batch.Commit())
	_, err = RunIncrementalBackup(t.Context(), testLogger(), store, mock, "policy", 0)
	require.NoError(t, err)
	retained, err = ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)
	require.Equal(t, published, retained)
}

func TestRestoreRejectsMalformedOperationalPolicyBeforeHistory(t *testing.T) {
	t.Parallel()
	source, store, _ := newSigningParityMachine(t)
	valid := source.State.ClusterPolicy.CloneVT()
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, valid))
	require.NoError(t, batch.Commit())
	for _, data := range [][]byte{{0xff}, {}, (&commonpb.ClusterPolicy{Revision: 1, QueryCheckpointLimit: 1}).MarshalDeterministicVT(nil)} {
		manifest := &Manifest{ClusterPolicy: &ClusterPolicySnapshot{Data: data}}
		err := ApplyExportsAndRebuild(t.Context(), testLogger(), newInMemoryBackupStorage(), store, manifest)
		require.Error(t, err)
		actual, readErr := query.ReadClusterPolicy(store)
		require.NoError(t, readErr)
		require.True(t, proto.Equal(valid, actual))
	}
}
