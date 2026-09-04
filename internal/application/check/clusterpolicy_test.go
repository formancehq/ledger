package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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

func collectClusterPolicyEvents(t *testing.T, store *dal.Store, v *clusterPolicyVerifier) []*servicepb.CheckStoreError {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	var got []*servicepb.CheckStoreError

	require.NoError(t, v.compare(handle, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			got = append(got, e.Error)
		}
	}))

	return got
}

// A stored policy that matches the folded SetClusterPolicy orders passes.
func TestClusterPolicyVerifier_Consistent(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	policy := &commonpb.ClusterPolicy{Revision: 3, IdempotencyTtlMicros: 1000, QueryCheckpointLimit: 5}
	writeClusterPolicyRow(t, store, policy)

	v := newClusterPolicyVerifier()
	v.applyOrder(setClusterPolicyOrder(policy))

	require.Empty(t, collectClusterPolicyEvents(t, store, v))
}

// Both absent — no policy audited, none stored — is consistent.
func TestClusterPolicyVerifier_BothAbsent(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	v := newClusterPolicyVerifier()

	require.Empty(t, collectClusterPolicyEvents(t, store, v))
}

// A stored policy with no audited SetClusterPolicy order is flagged as injected.
func TestClusterPolicyVerifier_InjectedFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeClusterPolicyRow(t, store, &commonpb.ClusterPolicy{Revision: 2, QueryCheckpointLimit: 3})

	v := newClusterPolicyVerifier()

	events := collectClusterPolicyEvents(t, store, v)
	require.Len(t, events, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_MISMATCH, events[0].GetErrorType())
}

// An audited policy with no stored row is flagged as lost.
func TestClusterPolicyVerifier_MissingFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	v := newClusterPolicyVerifier()
	v.applyOrder(setClusterPolicyOrder(&commonpb.ClusterPolicy{Revision: 4, QueryCheckpointLimit: 1}))

	events := collectClusterPolicyEvents(t, store, v)
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "revision 4")
}

// A stored policy whose contents diverge from the audited one is corruption.
func TestClusterPolicyVerifier_ContentMismatch(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeClusterPolicyRow(t, store, &commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 9})

	v := newClusterPolicyVerifier()
	v.applyOrder(setClusterPolicyOrder(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 5}))

	events := collectClusterPolicyEvents(t, store, v)
	require.Len(t, events, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_MISMATCH, events[0].GetErrorType())
}

// The fold keeps the highest revision regardless of the order orders arrive in.
func TestClusterPolicyVerifier_MaxRevisionWins(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	latest := &commonpb.ClusterPolicy{Revision: 5, QueryCheckpointLimit: 8}
	writeClusterPolicyRow(t, store, latest)

	v := newClusterPolicyVerifier()
	v.applyOrder(setClusterPolicyOrder(&commonpb.ClusterPolicy{Revision: 2, QueryCheckpointLimit: 1}))
	v.applyOrder(setClusterPolicyOrder(latest))
	v.applyOrder(setClusterPolicyOrder(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 1}))

	require.Empty(t, collectClusterPolicyEvents(t, store, v))
}

// A truncated fold (audit chain break) skips the comparison and reports
// coverage instead of mismatches it cannot substantiate.
func TestClusterPolicyVerifier_IncompleteReported(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeClusterPolicyRow(t, store, &commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 5})

	v := newClusterPolicyVerifier()
	v.markLiveTruncated()

	events := collectClusterPolicyEvents(t, store, v)
	require.Len(t, events, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_VERIFICATION_INCOMPLETE, events[0].GetErrorType())
}
