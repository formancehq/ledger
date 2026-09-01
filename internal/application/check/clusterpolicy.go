package check

import (
	"context"
	"errors"
	"fmt"
	"io"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
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
// applied policy — the fold keeps the max, which is order-independent and so
// needs neither oldest-first archived replay nor an archive-boundary skip.
//
// Like signingVerifier, the expectation is never seeded from the live projection
// or the baseline checkpoint (a verbatim copy of the projection under test):
// cold storage is the only audit-derived source for a policy whose last change
// predates the archive boundary. When that source is unavailable the run reports
// incomplete coverage rather than presenting a live-only replay as clean.
type clusterPolicyVerifier struct {
	// policy is the highest-revision SetClusterPolicy order folded so far, or nil
	// if none was seen (no policy ever committed).
	policy *commonpb.ClusterPolicy
	// coldComplete records that the archived audit range was replayed. Fail-closed:
	// a zero-value verifier reports incomplete coverage.
	coldComplete bool
	// liveTruncated records that the live audit fold stopped short of the range
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

// foldArchived replays the SetClusterPolicy orders of every archived chapter
// into the expectation and records whether that coverage is complete. Order is
// irrelevant (max-revision wins), so chapters are folded as they come.
//
// Every failure mode — no cold reader, a failed read, an undecodable order — is
// a coverage gap, not a checker failure: coldComplete stays false, compare
// reports the incomplete finding, and Check() carries on. The error return is
// always nil today; kept so a future caller-fatal condition needs no churn.
func (v *clusterPolicyVerifier) foldArchived(
	ctx context.Context,
	chapters []*commonpb.Chapter,
	coldReader *coldstorage.ColdReader,
	logger logging.Logger,
) error {
	var archived []*commonpb.Chapter

	for _, ch := range chapters {
		if ch.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			archived = append(archived, ch)
		}
	}

	if len(archived) == 0 {
		v.coldComplete = true

		return nil
	}

	if coldReader == nil {
		logger.Info("archived chapters exist but cold storage is unavailable; a cluster policy set before the archive boundary cannot be verified")

		return nil
	}

	for _, ch := range archived {
		coldPebble, err := coldReader.GetReader(ctx, ch.GetId())
		if err != nil {
			logger.Infof("reading archived chapter %d for cluster policy verification failed: %v", ch.GetId(), err)

			return nil
		}

		if err := v.foldChapter(ctx, coldPebble); err != nil {
			logger.Infof("folding cluster policy orders from archived chapter %d failed: %v", ch.GetId(), err)

			return nil
		}
	}

	v.coldComplete = true

	return nil
}

// foldChapter folds the SetClusterPolicy orders of one archived chapter's audit
// items into the expectation. Only SUCCESSFUL entries and only items inside the
// entry's fresh-log window are folded, mirroring the live fold.
func (v *clusterPolicyVerifier) foldChapter(ctx context.Context, reader dal.PebbleReader) error {
	entries, err := query.ReadAuditEntries(ctx, reader, nil)
	if err != nil {
		return fmt.Errorf("reading archived audit entries: %w", err)
	}

	defer func() { _ = entries.Close() }()

	for {
		entry, err := entries.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("reading archived audit entry: %w", err)
		}

		success := entry.GetSuccess()
		if success == nil {
			continue
		}

		items, err := query.ReadAuditItems(ctx, reader, entry.GetSequence())
		if err != nil {
			return fmt.Errorf("reading archived audit items for sequence %d: %w", entry.GetSequence(), err)
		}

		for _, item := range items {
			logSeq := item.GetLogSequence()
			if logSeq == 0 || logSeq < success.GetMinLogSequence() || logSeq > success.GetMaxLogSequence() {
				continue
			}

			order := &raftcmdpb.Order{}
			if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
				return fmt.Errorf("decoding order from archived audit item at log %d: %w", logSeq, err)
			}

			v.applyOrder(order)
		}
	}

	return nil
}

// compare reads the stored cluster policy and reports any divergence from the
// audit-derived expectation. An incomplete fold (unread archive or a truncated
// live range) reports coverage instead of a mismatch it cannot substantiate.
func (v *clusterPolicyVerifier) compare(reader dal.PebbleReader, callback func(*servicepb.CheckStoreEvent)) error {
	stored, err := query.ReadClusterPolicy(reader)
	if err != nil {
		return fmt.Errorf("reading the stored cluster policy: %w", err)
	}

	if !v.coldComplete || v.liveTruncated {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_CLUSTER_POLICY_VERIFICATION_INCOMPLETE,
			clusterPolicyIncompleteMessage(v.coldComplete, v.liveTruncated), 0, "", "", ""))

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

// clusterPolicyIncompleteMessage explains which part of the audit history went
// unread so an operator can tell a missing cold-storage backend from a broken
// hash chain.
func clusterPolicyIncompleteMessage(coldComplete, liveTruncated bool) string {
	const (
		prefix = "the cluster policy could not be verified over the whole history: "
		suffix = ". The comparison is skipped for this run rather than reported against a partial expectation"

		coldGap = "the archived audit range was not replayed, so a policy set before the archive boundary is unverified"
		liveGap = "the live audit range was cut short by a hash chain break, so any policy update after it is unread"
	)

	switch {
	case coldComplete:
		return prefix + liveGap + suffix
	case !liveTruncated:
		return prefix + coldGap + suffix
	default:
		return prefix + coldGap + "; the live range was also cut short by a hash chain break" + suffix
	}
}
