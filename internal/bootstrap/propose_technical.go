package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// maxTechnicalStaleRetries bounds the number of times proposeTechnical will
// retry an ErrStaleProposal rejection before giving up. Stale rejections
// happen when the IndexTracker is inflated from a dropped proposal (e.g.
// leadership transition); a fresh PredictedIndex is computed on every
// re-attempt, so once the tracker catches up the next try succeeds.
const maxTechnicalStaleRetries = 5

// proposeTechnical submits a technical Raft proposal through the preload
// runner. It blocks until the FSM applies. Used by callers that previously
// went through NodeProposer.ProposeProposal — cluster config updates,
// idempotency eviction, IndexReady notifications.
//
// Why route technical proposals through the preload runner:
//   - PredictedIndex is set as a backstop: a stale tracker (e.g. a
//     dropped proposal during leadership transition) causes the FSM
//     to reject with ErrStaleProposal rather than silently apply
//     against an inconsistent state.
//   - All proposals go through one canonical path, so the IndexTracker
//     lock and Raft propose are serialized identically everywhere.
//
// The caller is responsible for declaring `needs` — every key its FSM
// apply path will read from `Registry.X.Get(...)` must be in there. A
// missing entry turns the apply into a silent no-op (cache miss returns
// nil) and the proposal effectively does nothing. Use a fresh
// `preload.NewNeeds()` and add the keys your apply path needs; pass nil
// or an empty Needs if your apply path does no cache-keyed reads (e.g.
// cluster config, idempotency eviction).
func proposeTechnical(ctx context.Context, preloader *preload.Preloader, proposer preload.Proposer, cmd *raftcmdpb.Proposal, needs *preload.Needs) error {
	if needs == nil {
		needs = preload.NewNeeds()
	}

	var lastErr error

	for attempt := range maxTechnicalStaleRetries {
		// Reset the per-attempt fields so RunWithPreload assigns a
		// fresh ID and PredictedIndex on each retry (the previous
		// stale rejection left them populated).
		cmd.Id = 0
		cmd.PredictedIndex = 0
		cmd.Preload = nil

		err := proposeTechnicalOnce(ctx, preloader, proposer, cmd, needs)
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

func proposeTechnicalOnce(ctx context.Context, preloader *preload.Preloader, proposer preload.Proposer, cmd *raftcmdpb.Proposal, needs *preload.Needs) error {
	build, err := preloader.BuildPreloads(needs)
	if err != nil {
		if build != nil {
			build.ReleaseLoaders()
		}

		return fmt.Errorf("building preloads for technical proposal: %w", err)
	}

	result, err := preloader.RunWithPreload(
		ctx, cmd, build, needs,
		func(c *raftcmdpb.Proposal) ([]byte, error) { return c.MarshalVT() },
		proposer,
	)
	if err != nil {
		return err
	}

	result.Guard.ReleaseLoaders()

	// WaitContext (not Wait): callers running in lifecycle workers thread
	// a stop-derived context, and a node stopping or losing leadership
	// after Raft acceptance but before FSM apply would otherwise hang
	// these waits forever. Cancelling unblocks both and lets the caller
	// observe the shutdown / leadership change.
	if _, err := result.Proposal.WaitContext(ctx); err != nil {
		return fmt.Errorf("waiting for raft acceptance: %w", err)
	}

	res, err := result.FSMFuture.WaitContext(ctx)
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
	preloader *preload.Preloader
	proposer  preload.Proposer
}

func (a *indexReadyProposerAdapter) Propose(ctx context.Context, cmd *raftcmdpb.Proposal) error {
	// applyIndexReady reads `fsm.Registry.Ledgers.Get(ledgerKey)`; declare
	// the ledger names this proposal touches so the preload populates the
	// cache before apply.
	needs := preload.NewNeeds()
	for _, r := range cmd.GetIndexReadyUpdates() {
		if name := r.GetLedger(); name != "" {
			needs.Ledgers[domain.LedgerKey{Name: name}] = struct{}{}
		}
	}

	return proposeTechnical(ctx, a.preloader, a.proposer, cmd, needs)
}
