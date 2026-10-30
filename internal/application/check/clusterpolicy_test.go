package check

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func writeClusterPolicyRow(t *testing.T, store *dal.Store, policy *commonpb.ClusterPolicy) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, policy))
	require.NoError(t, batch.Commit())
}

func setClusterPolicyOrder(policy *commonpb.ClusterPolicy) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
					SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: policy},
				},
			},
		},
	}
}

// Operational policy is deliberately outside the business integrity contract.
// Historical policy orders still replay to preserve their original log slots.
func TestCheckClusterPolicyIsOperationalState(t *testing.T) {
	t.Parallel()
	for _, legacy := range []bool{false, true} {
		name := "native"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			machine, store, attrs := newFSMPurgeMachine(t)
			policy := machine.State.ClusterPolicy.CloneVT()
			policy.Revision = 2
			policy.MetadataMaxValueBytes = domain.DefaultMetadataMaxValueBytes * 2
			policy.MetadataMaxEntityBytes = max(policy.GetMetadataMaxEntityBytes(), policy.GetMetadataMaxValueBytes()*2)
			policy.MetadataMaxCommandBytes = max(policy.GetMetadataMaxCommandBytes(), policy.GetMetadataMaxEntityBytes())
			if legacy {
				applyFSMPurgeProposal(t, machine, store, attrs, 1, setClusterPolicyOrder(policy))
			} else {
				// No audit predecessor carries the live policy for native business replay.
				machine.State.UpdateClusterPolicy(policy)
				writeClusterPolicyRow(t, store, policy)
			}
			applyFSMPurgeProposal(t, machine, store, attrs, machine.State.LastAppliedIndex+1, fsmPurgeCreateLedgerOrder(commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL))
			applyFSMPurgeProposal(t, machine, store, attrs, machine.State.LastAppliedIndex+1,
				fsmPurgeSaveMetadataOrder("t-13:001", "large", strings.Repeat("x", int(domain.DefaultMetadataMaxValueBytes)+1)))
			require.NoError(t, store.Flush())
			require.Empty(t, runFSMPurgeCheck(t, store, attrs))
			// Even a malformed or absent operational row is not business corruption.
			writeClusterPolicyRow(t, store, &commonpb.ClusterPolicy{Revision: 99, MetadataMaxValueBytes: 1})
			require.Empty(t, runFSMPurgeCheck(t, store, attrs))
			batch := store.OpenWriteSession()
			require.NoError(t, batch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}))
			require.NoError(t, batch.Commit())
			require.Empty(t, runFSMPurgeCheck(t, store, attrs))
		})
	}
}

func TestCheckClusterPolicyWithoutBusinessHistory(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	writeClusterPolicyRow(t, store, &commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 5})
	_, _, attrs := newFSMPurgeMachine(t)
	require.Empty(t, runFSMPurgeCheck(t, store, attrs))
}
