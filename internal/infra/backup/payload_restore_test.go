package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestProposalPayloadPolicyIncrementalRestore(t *testing.T) {
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
	applyPolicy(4, true) // Native no-op is audited without consuming a log sequence.
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

	// The checker must derive policy from native audit intent, never from the
	// live policy projection or its companion log. Test each corruption alone.
	reader, err := restored.NewReadHandle()
	require.NoError(t, err)
	nativeItems, err := query.ReadAuditItems(t.Context(), reader, 3)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Len(t, nativeItems, 1)
	require.Empty(t, nativeItems[0].GetSerializedOrder())
	require.NotEmpty(t, nativeItems[0].GetSerializedSetClusterPolicy())
	tamperedPolicy := policy.CloneVT()
	tamperedPolicy.QueryCheckpointLimit++

	batch := restored.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, tamperedPolicy))
	require.NoError(t, batch.Commit())
	var found bool
	checker := check.NewChecker(restored, attributes.New(), nil, testLogger())
	require.NoError(t, checker.Check(t.Context(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok && e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_MISMATCH {
			found = true
		}
	}))
	require.True(t, found, "checker must detect policy projection tampering after a native delta")
	batch = restored.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, policy))
	require.NoError(t, batch.Commit())
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

	tamperedItem := nativeItems[0].CloneVT()
	tamperedItem.SerializedSetClusterPolicy = (&raftcmdpb.SetClusterPolicyProposal{Policy: tamperedPolicy}).MarshalDeterministicVT(nil)
	batch = restored.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(3).PutUint32(0).Consume()
	require.NoError(t, batch.SetProto(key, tamperedItem))
	require.NoError(t, batch.Commit())
	found = false
	require.NoError(t, checker.Check(t.Context(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok && e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH {
			found = true
		}
	}))
	require.True(t, found, "native policy audit bytes must be hash-bound")
	batch = restored.OpenWriteSession()
	require.NoError(t, batch.SetProto(key, nativeItems[0]))
	require.NoError(t, batch.Commit())
	auditKeyCheckerClean(t, restored)
}
