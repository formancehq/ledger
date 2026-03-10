package preload

import (
	"fmt"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

const maxPreloadRetries = 3

// Preloader manages the shared preload infrastructure used by both admission
// and mirror. It uses the Node's IndexTracker for accurate Raft index prediction
// and the Loaders for deduplication.
type Preloader struct {
	tracker *node.IndexTracker
	cache   *cache.Cache
	attrs   *attributes.Attributes
	store   *dal.Store
	loaders *Loaders
	logger  logging.Logger

	// proposeMu serializes the final boundary validation and Propose().
	// Preload building (the slow part) happens outside this lock;
	// only the fast critical section (validate + propose) is held.
	proposeMu sync.Mutex
}

// ProposalGuard holds the proposal lock and the cleanup token acquired by
// BuildAndValidatePreloads. It unifies two concerns:
//   - proposal lock: serializes boundary validation → Propose
//   - loader cleanup: tracks loaded keys for deduplication until the FSM applies
//
// The caller must call Release to unlock the proposal mutex, and ReleaseLoaders
// after the FSM applies (or immediately if loader dedup is not needed, e.g.
// mirror). ReleaseAll is a convenience for error paths.
type ProposalGuard struct {
	p     *Preloader
	token *CleanupToken
}

// Release releases the proposal lock. Must be called after Propose (success or
// failure) or when the proposal is abandoned. The Raft index is tracked by the
// Node's IndexTracker (incremented in Node.Propose).
// The loader cleanup token is NOT released — call ReleaseLoaders separately.
func (g *ProposalGuard) Release() {
	g.p.proposeMu.Unlock()
}

// ReleaseLoaders cleans up loader entries tracked during preload building.
// Safe to call multiple times (idempotent via nil check).
func (g *ProposalGuard) ReleaseLoaders() {
	if g.token != nil {
		g.token.Release(g.p.loaders)
		g.token = nil
	}
}

// ReleaseAll releases both the proposal lock and the loader entries.
// Convenience for error paths where both must be cleaned up.
func (g *ProposalGuard) ReleaseAll() {
	g.Release()
	g.ReleaseLoaders()
}

// New creates a Preloader using the given IndexTracker for Raft index prediction.
func New(tracker *node.IndexTracker, c *cache.Cache, attrs *attributes.Attributes, store *dal.Store, logger logging.Logger) *Preloader {
	return &Preloader{
		tracker: tracker,
		cache:   c,
		attrs:   attrs,
		store:   store,
		loaders: NewLoaders(),
		logger:  logger,
	}
}

// BuildAndValidatePreloads resolves all preload needs and acquires the proposal
// lock with a validated generation boundary. The returned ProposalGuard holds
// both the proposal lock and the loader cleanup token.
//
// Preload building (the slow part: store reads, loader dedup) happens outside
// the lock. Only the final boundary validation + lock acquisition is serialized,
// keeping the critical section to the fast marshal→Propose path.
//
// On error, the caller must call guard.ReleaseLoaders() if guard is non-nil
// (partial preload built before the error).
func (p *Preloader) BuildAndValidatePreloads(needs *Needs) (*raftcmdpb.PreloadSet, *ProposalGuard, error) {
	for attempt := range maxPreloadRetries {
		nextIndexBefore := p.tracker.Next()
		genBefore := p.cache.CurrentGeneration()

		preloadSet, token, err := p.buildPreloadsAt(nextIndexBefore, needs)
		if err != nil {
			guard := &ProposalGuard{p: p, token: token}

			return nil, guard, err
		}

		// Acquire the proposal lock and re-validate the boundary.
		// Under this lock, no other preloader can proceed to Propose.
		// The IndexTracker may still advance from non-preloader proposals,
		// but the boundary check below detects that.
		p.proposeMu.Lock()

		nextIndexAfter := p.tracker.Next()
		boundaryBefore := cache.BoundaryIndex(nextIndexBefore, p.cache.GenerationThreshold)
		boundaryAfter := cache.BoundaryIndex(nextIndexAfter, p.cache.GenerationThreshold)
		genAfter := p.cache.CurrentGeneration()

		// Validate both the predicted boundary and the actual cache generation.
		// The IndexTracker now tracks all Raft consumers, so boundary shifts
		// are rare but still possible during high concurrency.
		if boundaryBefore == boundaryAfter && genBefore == genAfter {
			return preloadSet, &ProposalGuard{p: p, token: token}, nil
		}

		// Generation changed — release lock and retry.
		p.proposeMu.Unlock()
		token.Release(p.loaders)
		p.logger.WithFields(map[string]any{
			"attempt":         attempt + 1,
			"max_retries":     maxPreloadRetries,
			"boundary_before": boundaryBefore,
			"boundary_after":  boundaryAfter,
			"gen_before":      genBefore,
			"gen_after":       genAfter,
		}).Infof("Preload validation failed: boundary or generation shifted, retrying")
	}

	// Exhausted retries — build under lock as last resort.
	p.logger.WithFields(map[string]any{
		"max_retries": maxPreloadRetries,
	}).Errorf("Preload validation exhausted retries — GenerationThreshold may be too low for current load")

	p.proposeMu.Lock()

	preloadSet, token, err := p.buildPreloadsAt(p.tracker.Next(), needs)
	if err != nil {
		p.proposeMu.Unlock()

		guard := &ProposalGuard{p: p, token: token}

		return nil, guard, err
	}

	return preloadSet, &ProposalGuard{p: p, token: token}, nil
}

// buildPreloadsAt resolves all preload needs at the given nextIndex.
func (p *Preloader) buildPreloadsAt(nextIndex uint64, needs *Needs) (*raftcmdpb.PreloadSet, *CleanupToken, error) {
	boundary := cache.BoundaryIndex(nextIndex, p.cache.GenerationThreshold)

	token := &CleanupToken{}
	preloadSet := &raftcmdpb.PreloadSet{
		LastPersistedIndex: boundary,
	}

	var err error

	// Ledgers
	if len(needs.Ledgers) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.Ledgers, err = resolveStandard(
			needs.Ledgers, nextIndex, boundary,
			p.cache.Ledgers, p.loaders.Ledgers,
			p.attrs.Ledger.ComputeValue, p.store,
			buildLedgerPreload, false, token.Ledgers,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading ledgers: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Boundaries
	if len(needs.Boundaries) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.Boundaries, err = resolveStandard(
			needs.Boundaries, nextIndex, boundary,
			p.cache.Boundaries, p.loaders.Boundaries,
			p.attrs.Boundary.ComputeValue, p.store,
			buildBoundaryPreload, false, token.Boundaries,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading boundaries: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Volumes — always send (zero values for new accounts)
	if len(needs.Volumes) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.Volumes, err = resolveStandard(
			needs.Volumes, nextIndex, boundary,
			p.cache.Volumes, p.loaders.Volumes,
			p.attrs.Volume.ComputeValue, p.store,
			buildVolumePreload, true, token.Volumes,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading volumes: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Idempotency keys — only send if found
	if len(needs.IdempotencyKeys) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.IdempotencyKeys, err = resolveStandard(
			needs.IdempotencyKeys, nextIndex, boundary,
			p.cache.IdempotencyKeys, p.loaders.IdempotencyKeys,
			p.attrs.IdempotencyKeys.ComputeValue, p.store,
			buildIdempotencyKeyPreload, false, token.IdempotencyKeys,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading idempotency keys: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// References — only send if found
	if len(needs.References) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.References, err = resolveStandard(
			needs.References, nextIndex, boundary,
			p.cache.References, p.loaders.References,
			p.attrs.References.ComputeValue, p.store,
			buildReferencePreload, false, token.References,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading references: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Sink configs — custom loader, only send if found
	if len(needs.SinkConfigs) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.SinkConfigs, err = resolveCustom(
			needs.SinkConfigs, nextIndex, boundary,
			p.cache.SinkConfigs, p.loaders.SinkConfigs,
			buildSinkConfigPreload, false, token.SinkConfigs,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading sink configs: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Numscript versions — custom loader, always send (empty = "not found")
	if len(needs.NumscriptVersions) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.NumscriptVersions, err = resolveCustom(
			needs.NumscriptVersions, nextIndex, boundary,
			p.cache.NumscriptVersions, p.loaders.NumscriptVersions,
			buildNumscriptVersionPreload, true, token.NumscriptVersions,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading numscript versions: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Numscript entries — custom loader, always send (both true/false needed)
	if len(needs.NumscriptEntries) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.NumscriptEntries, err = resolveCustom(
			needs.NumscriptEntries, nextIndex, boundary,
			p.cache.NumscriptEntries, p.loaders.NumscriptEntries,
			buildNumscriptEntryPreload, true, token.NumscriptEntries,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading numscript entries: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Transactions — only send if found
	if len(needs.Transactions) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.Transactions, err = resolveStandard(
			needs.Transactions, nextIndex, boundary,
			p.cache.Transactions, p.loaders.Transactions,
			p.attrs.Transaction.ComputeValue, p.store,
			buildTransactionStatePreload, false, token.Transactions,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading transactions: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	// Account metadata — only send if found
	if len(needs.Metadata) > 0 {
		var preloads []*raftcmdpb.Preload
		preloads, token.AccountMetadata, err = resolveStandard(
			needs.Metadata, nextIndex, boundary,
			p.cache.AccountMetadata, p.loaders.AccountMetadata,
			p.attrs.Metadata.ComputeValue, p.store,
			buildMetadataPreload, false, token.AccountMetadata,
		)
		if err != nil {
			return nil, token, fmt.Errorf("preloading account metadata: %w", err)
		}

		preloadSet.Preloads = append(preloadSet.Preloads, preloads...)
	}

	return preloadSet, token, nil
}
