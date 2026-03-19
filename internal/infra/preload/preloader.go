package preload

import (
	"sync"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

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

// PreloadBuild carries the result of an optimistic BuildPreloads call.
// The caller uses PreloadSet for marshalling outside the lock, then passes
// the build to AcquireProposalGuard for boundary validation.
type PreloadBuild struct {
	PreloadSet *raftcmdpb.PreloadSet
	token      *CleanupToken
	nextIndex  uint64
	generation uint64
}

// ReleaseLoaders releases the loader cleanup token from the build.
// Use this on error paths when AcquireProposalGuard was never called.
// Safe to call multiple times (idempotent via nil check).
func (b *PreloadBuild) ReleaseLoaders(loaders *Loaders) {
	if b.token != nil {
		b.token.Release(loaders)
		b.token = nil
	}
}

// ProposalGuard holds the proposal lock and the cleanup token acquired by
// AcquireProposalGuard. It unifies two concerns:
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

// Loaders returns the shared Loaders instance, allowing callers to release
// tokens from a PreloadBuild on error paths.
func (p *Preloader) Loaders() *Loaders {
	return p.loaders
}

// BuildPreloads resolves all preload needs optimistically without holding the
// proposal lock. The returned PreloadBuild contains the PreloadSet for
// marshalling and internal state for boundary validation.
//
// On error, the caller must call build.ReleaseLoaders(preloader.Loaders()).
func (p *Preloader) BuildPreloads(needs *Needs) (*PreloadBuild, error) {
	nextIndex := p.tracker.Next()
	gen := p.cache.CurrentGeneration()

	preloadSet, token, err := p.buildPreloadsAt(nextIndex, needs)
	if err != nil {
		return &PreloadBuild{token: token, nextIndex: nextIndex, generation: gen}, err
	}

	return &PreloadBuild{
		PreloadSet: preloadSet,
		token:      token,
		nextIndex:  nextIndex,
		generation: gen,
	}, nil
}

// AcquireProposalGuard acquires the proposal lock and validates that the cache
// generation boundary has not shifted since the optimistic BuildPreloads call.
//
// Returns (updatedPreloadSet, guard, error):
//   - updatedPreloadSet is nil when the boundary is stable (common case) — the
//     caller should use the original build.PreloadSet.
//   - updatedPreloadSet is non-nil when the boundary shifted (rare) — the caller
//     must re-marshal the command with the new preload set while still holding
//     the guard.
//
// On error, the caller must call guard.ReleaseAll() if guard is non-nil.
func (p *Preloader) AcquireProposalGuard(build *PreloadBuild, needs *Needs) (*raftcmdpb.PreloadSet, *ProposalGuard, error) {
	p.proposeMu.Lock()

	nextIndexAfter := p.tracker.Next()
	boundaryBefore := cache.BoundaryIndex(build.nextIndex, p.cache.GenerationThreshold)
	boundaryAfter := cache.BoundaryIndex(nextIndexAfter, p.cache.GenerationThreshold)
	genAfter := p.cache.CurrentGeneration()

	if boundaryBefore == boundaryAfter && build.generation == genAfter {
		return nil, &ProposalGuard{p: p, token: build.token}, nil
	}

	// --- Boundary shifted: re-build under lock ---
	// Under proposeMu, no other preloader can advance the tracker via Propose,
	// so the boundary is stable for the duration of the build.
	build.token.Release(p.loaders)

	p.logger.WithFields(map[string]any{
		"boundary_before": boundaryBefore,
		"boundary_after":  boundaryAfter,
		"gen_before":      build.generation,
		"gen_after":       genAfter,
	}).Debugf("Preload boundary shifted, rebuilding under lock")

	preloadSet, token, err := p.buildPreloadsAt(p.tracker.Next(), needs)
	if err != nil {
		p.proposeMu.Unlock()

		return nil, &ProposalGuard{p: p, token: token}, err
	}

	return preloadSet, &ProposalGuard{p: p, token: token}, nil
}

// preloadResult holds the output of a single attribute-type resolution goroutine.
type preloadResult struct {
	preloads []*raftcmdpb.Preload
	err      error
}

// buildPreloadsAt resolves all preload needs at the given nextIndex.
// Each attribute type uses independent caches and loaders, so they are resolved
// in parallel to reduce wall-clock time and shard lock hold duration.
func (p *Preloader) buildPreloadsAt(nextIndex uint64, needs *Needs) (*raftcmdpb.PreloadSet, *CleanupToken, error) {
	boundary := cache.BoundaryIndex(nextIndex, p.cache.GenerationThreshold)

	token := &CleanupToken{}

	// Each goroutine writes to a distinct results[slot] and a distinct token field.
	const maxTypes = 11
	results := make([]preloadResult, maxTypes)

	var wg sync.WaitGroup

	slot := 0

	// launch spawns a resolve goroutine that writes to results[slot].
	launch := func(fn func(i int)) {
		i := slot
		slot++

		wg.Go(func() {
			fn(i)
		})
	}

	if len(needs.Ledgers) > 0 {
		launch(func(i int) {
			results[i].preloads, token.Ledgers, results[i].err = resolveStandard(
				needs.Ledgers, nextIndex, boundary,
				p.cache.Ledgers, p.loaders.Ledgers,
				p.attrs.Ledger.ComputeValue, p.store,
				buildLedgerPreload, false, nil,
			)
		})
	}

	if len(needs.Boundaries) > 0 {
		launch(func(i int) {
			results[i].preloads, token.Boundaries, results[i].err = resolveStandard(
				needs.Boundaries, nextIndex, boundary,
				p.cache.Boundaries, p.loaders.Boundaries,
				p.attrs.Boundary.ComputeValue, p.store,
				buildBoundaryPreload, false, nil,
			)
		})
	}

	if len(needs.Volumes) > 0 {
		launch(func(i int) {
			results[i].preloads, token.Volumes, results[i].err = resolveStandard(
				needs.Volumes, nextIndex, boundary,
				p.cache.Volumes, p.loaders.Volumes,
				p.attrs.Volume.ComputeValue, p.store,
				buildVolumePreload, true, nil,
			)
		})
	}

	if len(needs.IdempotencyKeys) > 0 {
		launch(func(i int) {
			results[i].preloads, token.IdempotencyKeys, results[i].err = resolveStandard(
				needs.IdempotencyKeys, nextIndex, boundary,
				p.cache.IdempotencyKeys, p.loaders.IdempotencyKeys,
				p.attrs.IdempotencyKeys.ComputeValue, p.store,
				buildIdempotencyKeyPreload, false, nil,
			)
		})
	}

	if len(needs.References) > 0 {
		launch(func(i int) {
			results[i].preloads, token.References, results[i].err = resolveStandard(
				needs.References, nextIndex, boundary,
				p.cache.References, p.loaders.References,
				p.attrs.References.ComputeValue, p.store,
				buildReferencePreload, false, nil,
			)
		})
	}

	if len(needs.SinkConfigs) > 0 {
		launch(func(i int) {
			results[i].preloads, token.SinkConfigs, results[i].err = resolveCustom(
				needs.SinkConfigs, nextIndex, boundary,
				p.cache.SinkConfigs, p.loaders.SinkConfigs,
				buildSinkConfigPreload, false, nil,
			)
		})
	}

	if len(needs.NumscriptVersions) > 0 {
		launch(func(i int) {
			results[i].preloads, token.NumscriptVersions, results[i].err = resolveCustom(
				needs.NumscriptVersions, nextIndex, boundary,
				p.cache.NumscriptVersions, p.loaders.NumscriptVersions,
				buildNumscriptVersionPreload, true, nil,
			)
		})
	}

	if len(needs.NumscriptEntries) > 0 {
		launch(func(i int) {
			results[i].preloads, token.NumscriptEntries, results[i].err = resolveCustom(
				needs.NumscriptEntries, nextIndex, boundary,
				p.cache.NumscriptEntries, p.loaders.NumscriptEntries,
				buildNumscriptEntryPreload, true, nil,
			)
		})
	}

	if len(needs.NumscriptParsed) > 0 {
		launch(func(i int) {
			results[i].preloads, token.NumscriptParsed, results[i].err = resolveCustom(
				needs.NumscriptParsed, nextIndex, boundary,
				p.cache.NumscriptParsed, p.loaders.NumscriptParsed,
				buildNumscriptParsedPreload, true, nil,
			)
		})
	}

	if len(needs.Transactions) > 0 {
		launch(func(i int) {
			results[i].preloads, token.Transactions, results[i].err = resolveStandard(
				needs.Transactions, nextIndex, boundary,
				p.cache.Transactions, p.loaders.Transactions,
				p.attrs.Transaction.ComputeValue, p.store,
				buildTransactionStatePreload, false, nil,
			)
		})
	}

	if len(needs.Metadata) > 0 {
		launch(func(i int) {
			results[i].preloads, token.AccountMetadata, results[i].err = resolveStandard(
				needs.Metadata, nextIndex, boundary,
				p.cache.AccountMetadata, p.loaders.AccountMetadata,
				p.attrs.Metadata.ComputeValue, p.store,
				buildMetadataPreload, false, nil,
			)
		})
	}

	wg.Wait()

	// Merge results, returning first error encountered.
	preloadSet := &raftcmdpb.PreloadSet{
		LastPersistedIndex: boundary,
	}

	for i := range slot {
		if results[i].err != nil {
			return nil, token, results[i].err
		}

		preloadSet.Preloads = append(preloadSet.Preloads, results[i].preloads...)
	}

	return preloadSet, token, nil
}
