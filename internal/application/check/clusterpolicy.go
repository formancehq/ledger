package check

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// clusterPolicyVerifier re-derives the persisted cluster policy (the
// SubGlobClusterPolicy row) from chain-bound SetClusterPolicy orders and
// compares it to what is stored. It is an invariant-#8 governance projection:
// state.Recovery loads it straight into FSMState, where the FSM apply path
// consults it to gate idempotency expiration and query-checkpoint admission, so
// a disk edit changes committed behavior.
//
// The policy is a single latest-wins value with a monotonic revision. Only an
// accepted update (a strictly higher revision) emits a log, so every revision
// appears at most once in the chain and the highest-revision order is the
// applied policy — the fold keeps the max, which is order-independent.
//
// Like signingVerifier, the expectation is never seeded from the live
// projection: the audit chain is the only audit-derived source.
type clusterPolicyVerifier struct {
	// policy is the highest-revision SetClusterPolicy order folded so far, or nil
	// if none was seen (no policy ever committed).
	policy *commonpb.ClusterPolicy
	// liveTruncated records that the audit fold stopped short of the range
	// end (an audit chain break); the accumulated prefix cannot be compared.
	liveTruncated bool
}

func newClusterPolicyVerifier() *clusterPolicyVerifier {
	return &clusterPolicyVerifier{}
}

// markLiveTruncated records that the live audit fold stopped short of the end of
// the range. Called from every non-error early exit in verifyAuditHashChain.
func (v *clusterPolicyVerifier) markLiveTruncated() {
	v.liveTruncated = true
}

// applyOrder folds one order into the expectation, keeping the highest revision
// seen. Callers MUST only pass orders from SUCCESSFUL audit entries: a rejected
// order left no trace in the projection.
func (v *clusterPolicyVerifier) applyOrder(order *raftcmdpb.Order) {
	payload, ok := order.GetSystemScoped().GetPayload().(*raftcmdpb.SystemScopedOrder_SetClusterPolicy)
	if !ok {
		return
	}

	policy := payload.SetClusterPolicy.GetPolicy()
	if policy == nil {
		return
	}

	if v.policy == nil || policy.GetRevision() > v.policy.GetRevision() {
		// Cloned because the order may be reused or mutated after this returns.
		v.policy = policy.CloneVT()
	}
}

// compare reads the stored cluster policy and reports any divergence from the
// audit-derived expectation. An incomplete fold (a truncated live range)
// reports coverage instead of a mismatch it cannot substantiate.
func (v *clusterPolicyVerifier) compare(reader dal.PebbleReader, callback func(*servicepb.CheckStoreEvent)) error {
	stored, err := query.ReadClusterPolicy(reader)
	if err != nil {
		return fmt.Errorf("reading the stored cluster policy: %w", err)
	}

	if v.liveTruncated {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_VERIFICATION_INCOMPLETE,
			"the cluster policy could not be verified over the whole history: the audit range was cut "+
				"short by a hash chain break, so any policy update after it is unread. The comparison is "+
				"skipped for this run rather than reported against a partial expectation",
			0, "", "", ""))

		return nil
	}

	switch {
	case v.policy == nil && stored == nil:
		// No policy ever committed and none stored: consistent.
	case v.policy == nil:
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_MISMATCH,
			"a cluster policy is stored but no audited SetClusterPolicy order set one (injected, or an audited update was lost)",
			0, "", "", ""))
	case stored == nil:
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_MISMATCH,
			fmt.Sprintf("the audited cluster policy (revision %d) is missing from the store", v.policy.GetRevision()),
			0, "", "", ""))
	case !v.policy.EqualVT(stored):
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_MISMATCH,
			fmt.Sprintf("the stored cluster policy (revision %d) differs from the audited SetClusterPolicy orders (revision %d)",
				stored.GetRevision(), v.policy.GetRevision()),
			0, "", "", ""))
	}

	return nil
}
