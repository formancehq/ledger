package preload

import (
	"fmt"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/bloom"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Preloader manages the shared preload infrastructure used by both admission
// and mirror. It uses the Node's IndexTracker for accurate Raft index prediction
// and the Loaders for deduplication.
//
// The proposal lock is the IndexTracker's own mutex (tracker.Lock/Unlock),
// which also serializes with IndexTracker.Decrement. This prevents a race
// where a dropped proposal shifts the tracker across a generation boundary
// between AcquireProposalGuard's validation and Node.Propose.
type Preloader struct {
	tracker      *node.IndexTracker
	cache        *cache.Cache
	attrs        *attributes.Attributes
	store        *dal.Store
	loaders      *Loaders
	bloomFilters *bloom.FilterSet
	logger       logging.Logger
}

// PreloadBuild carries the result of an optimistic BuildPreloads call.
// The caller uses PreloadSet for marshalling outside the lock, then passes
// the build to AcquireProposalGuard for generation validation.
type PreloadBuild struct {
	PreloadSet   *raftcmdpb.PreloadSet
	token        *CleanupToken
	nextIndex    uint64
	nextIndexGen uint64 // gen(nextIndex, threshold) — the future generation used by CheckCache
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
	g.p.tracker.Unlock()
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
func New(tracker *node.IndexTracker, c *cache.Cache, attrs *attributes.Attributes, store *dal.Store, bloomFilters *bloom.FilterSet, logger logging.Logger) *Preloader {
	return &Preloader{
		tracker:      tracker,
		cache:        c,
		attrs:        attrs,
		store:        store,
		loaders:      NewLoaders(),
		bloomFilters: bloomFilters,
		logger:       logger,
	}
}

// Loaders returns the shared Loaders instance, allowing callers to release
// tokens from a PreloadBuild on error paths.
func (p *Preloader) Loaders() *Loaders {
	return p.loaders
}

// ReadBoundaries reads the current LedgerBoundaries for the given ledger
// directly from Pebble (not from the cache overlay).
func (p *Preloader) ReadBoundaries(ledgerName string) (*raftcmdpb.LedgerBoundaries, error) {
	reader, err := p.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = reader.Close() }()

	return p.attrs.Boundary.Get(reader, []byte(ledgerName))
}

// TrackerNext returns the predicted next Raft index from the IndexTracker.
func (p *Preloader) TrackerNext() uint64 { return p.tracker.Next() }

// LockTracker acquires the IndexTracker's mutex. Used by non-guard callers
// (mirror error reporting, admission barrier) that Propose without going
// through AcquireProposalGuard, to serialize the tracker Increment with
// guarded proposals.
func (p *Preloader) LockTracker() { p.tracker.Lock() }

// UnlockTracker releases the IndexTracker's mutex.
func (p *Preloader) UnlockTracker() { p.tracker.Unlock() }

// BuildPreloads resolves all preload needs optimistically without holding the
// proposal lock. The returned PreloadBuild contains the PreloadSet for
// marshalling and internal state for boundary validation.
//
// On error, the caller must call build.ReleaseLoaders(preloader.Loaders()).
func (p *Preloader) BuildPreloads(needs *Needs) (*PreloadBuild, error) {
	nextIndex := p.tracker.Next()
	nextIndexGen := cache.Gen(nextIndex, p.cache.GenerationThreshold)

	preloadSet, token, err := p.buildPreloadsAt(nextIndex, needs)
	if err != nil {
		return &PreloadBuild{token: token, nextIndex: nextIndex, nextIndexGen: nextIndexGen}, err
	}

	return &PreloadBuild{
		PreloadSet:   preloadSet,
		token:        token,
		nextIndex:    nextIndex,
		nextIndexGen: nextIndexGen,
	}, nil
}

// AcquireProposalGuard acquires the proposal lock and validates that
// gen(nextIndex) has not changed since the optimistic BuildPreloads call.
//
// CheckCache uses gen(nextIndex) as the future generation to decide whether
// cached data will survive. Under the proposal lock, nextIndex is stable
// (no other goroutine can Propose), so gen(nextIndex) is deterministic.
// The cache's currentGeneration (FSM side) is NOT checked here — it can
// advance at any time and is read atomically by CheckCache itself.
//
// Returns (updatedPreloadSet, guard, error):
//   - updatedPreloadSet is nil when stable (common case) — use build.PreloadSet.
//   - updatedPreloadSet is non-nil when stale (rare) — re-marshal with it.
//
// On error, the caller must call guard.ReleaseAll() if guard is non-nil.
func (p *Preloader) AcquireProposalGuard(build *PreloadBuild, needs *Needs) (*raftcmdpb.PreloadSet, *ProposalGuard, error) {
	p.tracker.Lock()

	nextIndexAfter := p.tracker.Next()
	nextIndexGenAfter := cache.Gen(nextIndexAfter, p.cache.GenerationThreshold)

	if build.nextIndexGen == nextIndexGenAfter {
		return nil, &ProposalGuard{p: p, token: build.token}, nil
	}

	// --- Stale: re-build under lock ---
	// nextIndex crossed a generation boundary since BuildPreloads, so the
	// CheckCache decisions (hit/miss/touch) may be wrong. Under the tracker
	// lock, nextIndex is stable for the duration of the rebuild (Decrement
	// also acquires this lock, preventing the race).
	build.token.Release(p.loaders)

	if p.logger.Enabled(logging.DebugLevel) {
		p.logger.WithFields(map[string]any{
			"nextIndex_before":    build.nextIndex,
			"nextIndex_after":     nextIndexAfter,
			"nextIndexGen_before": build.nextIndexGen,
			"nextIndexGen_after":  nextIndexGenAfter,
		}).Debugf("nextIndex generation changed, rebuilding preloads under lock")
	}

	preloadSet, token, err := p.buildPreloadsAt(p.tracker.Next(), needs)
	if err != nil {
		p.tracker.Unlock()

		return nil, &ProposalGuard{p: p, token: token}, err
	}

	return preloadSet, &ProposalGuard{p: p, token: token}, nil
}

// bloomFilter returns the bloom filter for the given attribute type, or nil if disabled.
func (p *Preloader) bloomFilter(attrType byte) *bloom.Filter {
	if p.bloomFilters == nil || !p.bloomFilters.IsReady() {
		return nil
	}

	return p.bloomFilters.FilterForAttrType(attrType)
}

// buildResult holds the output of a single attribute-type resolution goroutine.
type buildResult struct {
	resolve *resolveResult
	err     error
}

// buildPreloadsAt resolves all preload needs at the given nextIndex.
// Each attribute type uses independent caches and loaders, so they are resolved
// in parallel to reduce wall-clock time and shard lock hold duration.
func (p *Preloader) buildPreloadsAt(nextIndex uint64, needs *Needs) (*raftcmdpb.PreloadSet, *CleanupToken, error) {
	boundary := cache.BoundaryIndex(nextIndex, p.cache.GenerationThreshold)

	if p.logger.Enabled(logging.DebugLevel) {
		p.logger.WithFields(map[string]any{
			"nextIndex":           nextIndex,
			"boundary":            boundary,
			"generationThreshold": p.cache.GenerationThreshold,
			"gen":                 cache.Gen(nextIndex, p.cache.GenerationThreshold),
		}).Debugf("Preloader: buildPreloadsAt")
	}

	token := &CleanupToken{}

	// Each goroutine writes to a distinct results[slot] and a distinct token field.
	const maxTypes = 11
	results := make([]buildResult, maxTypes)

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
			var r *resolveResult
			r, results[i].err = resolveStandard(
				needs.Ledgers, nextIndex, boundary,
				p.cache.Ledgers, p.loaders.Ledgers,
				p.attrs.Ledger.Get, p.store,
				buildLedgerPreload, true,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_LEDGERS, nil,
				p.bloomFilter(dal.AttributePrefixLedger),
				p.logger, "ledgers",
			)
			results[i].resolve = r
			if r != nil {
				token.Ledgers = r.tracker
			}
		})
	}

	if len(needs.Boundaries) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveStandard(
				needs.Boundaries, nextIndex, boundary,
				p.cache.Boundaries, p.loaders.Boundaries,
				p.attrs.Boundary.Get, p.store,
				buildBoundaryPreload, true,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_BOUNDARIES, nil,
				p.bloomFilter(dal.AttributePrefixBoundary),
				p.logger, "boundaries",
			)
			results[i].resolve = r
			if r != nil {
				token.Boundaries = r.tracker
			}
		})
	}

	if len(needs.Volumes) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveStandard(
				needs.Volumes, nextIndex, boundary,
				p.cache.Volumes, p.loaders.Volumes,
				p.attrs.Volume.Get, p.store,
				buildVolumePreload, true,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_VOLUMES, nil,
				p.bloomFilter(dal.AttributePrefixVolume),
				p.logger, "volumes",
			)
			results[i].resolve = r
			if r != nil {
				token.Volumes = r.tracker
			}
		})
	}

	if len(needs.IdempotencyKeys) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveStandard(
				needs.IdempotencyKeys, nextIndex, boundary,
				p.cache.IdempotencyKeys, p.loaders.IdempotencyKeys,
				p.attrs.IdempotencyKeys.Get, p.store,
				buildIdempotencyKeyPreload, false,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_IDEMPOTENCY_KEYS, nil,
				p.bloomFilter(dal.AttributePrefixIdempotency),
				p.logger, "idempotency_keys",
			)
			results[i].resolve = r
			if r != nil {
				token.IdempotencyKeys = r.tracker
			}
		})
	}

	if len(needs.References) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveStandard(
				needs.References, nextIndex, boundary,
				p.cache.References, p.loaders.References,
				p.attrs.References.Get, p.store,
				buildReferencePreload, false,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_REFERENCES, nil,
				p.bloomFilter(dal.AttributePrefixReference),
				p.logger, "references",
			)
			results[i].resolve = r
			if r != nil {
				token.References = r.tracker
			}
		})
	}

	if len(needs.SinkConfigs) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveCustom(
				needs.SinkConfigs, nextIndex, boundary,
				p.cache.SinkConfigs, p.loaders.SinkConfigs,
				buildSinkConfigPreload, false,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_SINK_CONFIGS, nil,
				p.logger, "sink_configs",
			)
			results[i].resolve = r
			if r != nil {
				token.SinkConfigs = r.tracker
			}
		})
	}

	if len(needs.NumscriptVersions) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveCustom(
				needs.NumscriptVersions, nextIndex, boundary,
				p.cache.NumscriptVersions, p.loaders.NumscriptVersions,
				buildNumscriptVersionPreload, true,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_NUMSCRIPT_VERSIONS, nil,
				p.logger, "numscript_versions",
			)
			results[i].resolve = r
			if r != nil {
				token.NumscriptVersions = r.tracker
			}
		})
	}

	if len(needs.NumscriptEntries) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveCustom(
				needs.NumscriptEntries, nextIndex, boundary,
				p.cache.NumscriptEntries, p.loaders.NumscriptEntries,
				buildNumscriptEntryPreload, true,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_NUMSCRIPT_ENTRIES, nil,
				p.logger, "numscript_entries",
			)
			results[i].resolve = r
			if r != nil {
				token.NumscriptEntries = r.tracker
			}
		})
	}

	if len(needs.NumscriptParsed) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveCustom(
				needs.NumscriptParsed, nextIndex, boundary,
				p.cache.NumscriptParsed, p.loaders.NumscriptParsed,
				buildNumscriptParsedPreload, true,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_NUMSCRIPT_PARSED, nil,
				p.logger, "numscript_parsed",
			)
			results[i].resolve = r
			if r != nil {
				token.NumscriptParsed = r.tracker
			}
		})
	}

	if len(needs.Transactions) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveStandard(
				needs.Transactions, nextIndex, boundary,
				p.cache.Transactions, p.loaders.Transactions,
				p.attrs.Transaction.Get, p.store,
				buildTransactionStatePreload, false,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_TRANSACTIONS, nil,
				p.bloomFilter(dal.AttributePrefixTransaction),
				p.logger, "transactions",
			)
			results[i].resolve = r
			if r != nil {
				token.Transactions = r.tracker
			}
		})
	}

	if len(needs.Metadata) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveStandard(
				needs.Metadata, nextIndex, boundary,
				p.cache.AccountMetadata, p.loaders.AccountMetadata,
				p.attrs.Metadata.Get, p.store,
				buildMetadataPreload, false,
				raftcmdpb.CacheTouchType_CACHE_TOUCH_ACCOUNT_METADATA, nil,
				p.bloomFilter(dal.AttributePrefixMetadata),
				p.logger, "metadata",
			)
			results[i].resolve = r
			if r != nil {
				token.AccountMetadata = r.tracker
			}
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

		if results[i].resolve != nil {
			preloadSet.Preloads = append(preloadSet.Preloads, results[i].resolve.preloads...)
			preloadSet.Touches = append(preloadSet.Touches, results[i].resolve.touches...)
		}
	}

	return preloadSet, token, nil
}
