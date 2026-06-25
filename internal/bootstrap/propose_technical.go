package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// maxTechnicalStaleRetries bounds the number of times proposeTechnical will
// retry an ErrStaleProposal rejection before giving up. Stale rejections
// happen when the IndexTracker is inflated from a dropped proposal (e.g.
// leadership transition); a fresh PredictedIndex is computed on every
// re-attempt, so once the tracker catches up the next try succeeds.
const maxTechnicalStaleRetries = 5

// proposeTechnical submits a technical Raft proposal through the
// preload runner. It blocks until the FSM applies. Used by callers
// that previously went through NodeProposer.ProposeProposal — cluster
// config updates, idempotency eviction, IndexReady notifications.
//
// Why route technical proposals through the preload runner:
//   - PredictedIndex is set as a backstop: a stale tracker (e.g. a
//     dropped proposal during leadership transition) causes the FSM
//     to reject with ErrStaleProposal rather than silently apply
//     against an inconsistent state.
//   - All proposals go through one canonical path, so the IndexTracker
//     lock and Raft propose are serialized identically everywhere.
//
// The caller passes one plan.WriteOperation per TechnicalUpdate in the
// proposal. Each operation declares the cache keys its handler will
// read (or nil/empty Needs for handlers that read nothing — cluster
// config, idempotency eviction) and a SetCoverage closure that the
// runner uses to write the bitset onto the corresponding
// TechnicalUpdate. Per-TU isolation is therefore structural even when
// a future batch carries multiple heterogeneous TUs.
func proposeTechnical(ctx context.Context, builder *plan.Builder, proposer plan.Proposer, cmd *raftcmdpb.Proposal, operations []plan.WriteOperation) error {
	var lastErr error

	for attempt := range maxTechnicalStaleRetries {
		// Reset the per-attempt fields so Run assigns a fresh ID and
		// PredictedIndex on each retry (the previous stale rejection
		// left them populated).
		cmd.Id = 0
		cmd.PredictedIndex = 0
		cmd.ExecutionPlan = nil

		err := proposeTechnicalOnce(ctx, builder, proposer, cmd, operations)
		if err == nil {
			return nil
		}

		if !errors.Is(err, domain.ErrStaleProposal) {
			return err
		}

		lastErr = err
		_ = attempt // for clarity; the loop counter bounds retries
	}

	return fmt.Errorf("proposeTechnical: giving up after %d stale retries: %w", maxTechnicalStaleRetries, lastErr)
}

func proposeTechnicalOnce(ctx context.Context, builder *plan.Builder, proposer plan.Proposer, cmd *raftcmdpb.Proposal, operations []plan.WriteOperation) error {
	build, err := builder.Build(operations)
	if err != nil {
		if build != nil {
			build.ReleaseLoaders()
		}

		return fmt.Errorf("building preloads for technical proposal: %w", err)
	}

	result, err := builder.Run(
		ctx, cmd, build,
		func(c *raftcmdpb.Proposal) ([]byte, error) { return c.MarshalVT() },
		proposer,
	)
	if err != nil {
		return err
	}

	result.Guard.ReleaseLoaders()

	return waitTechnical(ctx, result)
}

// waitTechnical blocks on Raft acceptance then FSM apply, returning the
// first error encountered. The caller's ctx (typically derived from a
// stop channel) cancels the wait when the node stops or loses leadership
// after Raft acceptance but before FSM apply, letting the caller observe
// the shutdown instead of hanging forever.
func waitTechnical(ctx context.Context, result *plan.RunResult) error {
	if _, err := result.Proposal.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for raft acceptance: %w", err)
	}

	res, err := result.FSMFuture.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for FSM apply: %w", err)
	}

	if res.Error != nil {
		// Wrap with %w so callers (and proposeTechnical's retry loop)
		// can detect ErrStaleProposal via errors.Is.
		return fmt.Errorf("applying technical proposal: %w", res.Error)
	}

	return nil
}

// indexReadyProposerAdapter satisfies indexbuilder.Proposer by routing
// each `IndexReadyUpdates` proposal through proposeTechnical with the
// `Ledgers` keys its apply path will read. Lives here (not in
// indexbuilder) because preload depends on state for ApplyResult and the
// indexbuilder package shouldn't have to know about preload internals.
type indexReadyProposerAdapter struct {
	builder  *plan.Builder
	proposer plan.Proposer
}

func (a *indexReadyProposerAdapter) Propose(ctx context.Context, cmd *raftcmdpb.Proposal) error {
	// applyIndexReady reads both `fsm.Registry.Ledgers.Get(name)` (to soft-
	// skip when the ledger is deleted — processDeleteLedger leaves the
	// Index cache entry live and deleteLedgerData purges Pebble out-of-
	// band, so without this check a racing IndexReady could resurrect an
	// orphan entry post-cleanup) AND `fsm.Registry.Indexes.Get(idxKey)`
	// via `indexes.Find(scope, ledgerName, id)`. The indexbuilder carries
	// the ledger name on the IndexReadyUpdate, so the proposer declares
	// both preloads directly — no chained name→ID resolution required.
	// One WriteOperation per TU with its own narrow Needs so the gate
	// rejects any cross-index or cross-ledger read at FSM time.
	tus := cmd.GetTechnicalUpdates()
	operations := make([]plan.WriteOperation, 0, len(tus))
	for i, tu := range tus {
		var needs *plan.Needs
		if kind, ok := tu.GetKind().(*raftcmdpb.TechnicalUpdate_IndexReady); ok {
			ledgerName := kind.IndexReady.GetLedger()
			if id := kind.IndexReady.GetId(); id != nil && ledgerName != "" {
				needs = plan.NewNeeds()
				needs.Indexes[domain.IndexKey{
					LedgerName: ledgerName,
					Canonical:  indexes.Canonical(id),
				}] = struct{}{}
				needs.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
			}
		}

		operations = append(operations, plan.WriteOperation{
			Needs: needs,
			SetCoverage: func(bits []byte) {
				cmd.GetTechnicalUpdates()[i].CoverageBits = bits
			},
		})
	}

	return proposeTechnical(ctx, a.builder, a.proposer, cmd, operations)
}
