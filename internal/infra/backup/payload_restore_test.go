package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestProposalPayloadPolicyCheckpointRestore(t *testing.T) {
	t.Parallel()
	for _, legacyDelta := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_delta_%t", legacyDelta), func(t *testing.T) {
			t.Parallel()
			source, store, _ := newCreationTestMachine(t)
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
			_, err := RunBackup(t.Context(), logging.Testing(), store, storage, "payload", "full")
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
			second, err := RunBackup(t.Context(), logging.Testing(), store, storage, "payload", "second")
			require.NoError(t, err)
			require.NotNil(t, second)
			manifest, err := ReadManifest(t.Context(), storage, ManifestKey("payload"))
			require.NoError(t, err)
			require.Empty(t, manifest.Exports)
			require.Nil(t, manifest.ClusterPolicy)
			restored := restoreAuditKeyCheckpoint(t, storage, manifest)
			actual, err := query.ReadClusterPolicy(restored)
			require.NoError(t, err)
			require.True(t, proto.Equal(policy, actual), "second checkpoint must carry updated policy")
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

// A policy-only mutation still changes the complete checkpoint. A failed
// manifest publication leaves the prior restore point intact for retry.
func TestPolicyOnlyBackupPublicationAndRetry(t *testing.T) {
	t.Parallel()
	source, store, _ := newCreationTestMachine(t)
	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), logging.Testing(), store, storage, "policy", "first")
	require.NoError(t, err)
	old, err := ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)

	policy := source.State.ClusterPolicy.CloneVT()
	policy.Revision++
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, policy))
	require.NoError(t, state.SetAppliedIndex(batch, 10))
	require.NoError(t, batch.Commit())

	mock := NewMockStorage(gomock.NewController(t))
	mock.EXPECT().GetFile(gomock.Any(), gomock.Any()).DoAndReturn(storage.GetFile).AnyTimes()
	mock.EXPECT().ListFiles(gomock.Any(), gomock.Any()).DoAndReturn(storage.ListFiles).AnyTimes()
	mock.EXPECT().DeleteFile(gomock.Any(), gomock.Any()).DoAndReturn(storage.DeleteFile).AnyTimes()
	failed := false
	mock.EXPECT().PutFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, key string, data io.Reader, size int64) error {
			if key == ManifestKey("policy") && !failed {
				failed = true

				return errors.New("manifest unavailable")
			}

			return storage.PutFile(ctx, key, data, size)
		}).AnyTimes()

	_, err = RunBackup(t.Context(), logging.Testing(), store, mock, "policy", "failed")
	require.ErrorContains(t, err, "manifest unavailable")
	retained, err := ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)
	require.Equal(t, old, retained)

	result, err := RunBackup(t.Context(), logging.Testing(), store, mock, "policy", "retry")
	require.NoError(t, err)
	require.Equal(t, uint64(10), result.LastAppliedIndex)
	published, err := ReadManifest(t.Context(), storage, ManifestKey("policy"))
	require.NoError(t, err)
	require.Nil(t, published.ClusterPolicy)
	require.Equal(t, uint64(10), published.Checkpoint.LastAppliedIndex)
	restored := restoreAuditKeyCheckpoint(t, storage, published)
	actual, err := query.ReadClusterPolicy(restored)
	require.NoError(t, err)
	require.True(t, proto.Equal(policy, actual))
}
