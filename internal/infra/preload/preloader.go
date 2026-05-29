package preload

import (
	"fmt"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
func (b *PreloadBuild) ReleaseLoaders() {
	if b.token != nil {
		b.token.Release()
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
		g.token.Release()
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

// ResolveLedgerID resolves a ledger name to its uint32 ID using the standard
// attribute resolution path: bloom → cache → Pebble.
// Returns (0, false) if the ledger does not exist.
func (p *Preloader) ResolveLedgerID(name string) (uint32, bool) {
	canonical := domain.LedgerKey{Name: name}.Bytes()
	id, _ := attributes.MakeKey(attributes.DefaultSeeds, canonical)

	// 1. Bloom filter: if definitely absent, skip.
	if p.bloomFilters != nil {
		if bf := p.bloomFilters.FilterForAttrType(dal.SubAttrLedger); bf != nil && !bf.MayContain(id) {
			return 0, false
		}
	}

	// 2. Cache: check gen0/gen1.
	if entry, ok := p.cache.Ledgers.Get(id); ok && entry.Data != nil {
		return entry.Data.GetId(), true
	}

	// 3. Pebble fallback (single point read, no snapshot needed).
	info, err := p.attrs.Ledger.Get(p.store, canonical)
	if err != nil || info == nil {
		return 0, false
	}

	return info.GetId(), true
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
// On error, the caller must call build.ReleaseLoaders().
func (p *Preloader) BuildPreloads(needs *Needs) (*PreloadBuild, error) {
	nextIndex := p.tracker.Next()
	snap := p.cache.Snapshot()
	nextIndexGen := cache.Gen(nextIndex, snap.GenerationThreshold)

	preloadSet, token, err := p.buildPreloadsAt(nextIndex, snap, needs)
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

	snap := p.cache.Snapshot()
	nextIndexAfter := p.tracker.Next()
	nextIndexGenAfter := cache.Gen(nextIndexAfter, snap.GenerationThreshold)

	if build.nextIndexGen == nextIndexGenAfter {
		return nil, &ProposalGuard{p: p, token: build.token}, nil
	}

	// --- Stale: re-build under lock ---
	// nextIndex crossed a generation boundary since BuildPreloads, so the
	// CheckCache decisions (hit/miss/touch) may be wrong. Under the tracker
	// lock, nextIndex is stable for the duration of the rebuild (Decrement
	// also acquires this lock, preventing the race).
	build.token.Release()

	if p.logger.Enabled(logging.DebugLevel) {
		p.logger.WithFields(map[string]any{
			"nextIndex_before":    build.nextIndex,
			"nextIndex_after":     nextIndexAfter,
			"nextIndexGen_before": build.nextIndexGen,
			"nextIndexGen_after":  nextIndexGenAfter,
		}).Debugf("nextIndex generation changed, rebuilding preloads under lock")
	}

	preloadSet, token, err := p.buildPreloadsAt(p.tracker.Next(), snap, needs)
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
	loader  LoaderOps // which loader the tracker keys should be released from
}

// buildPreloadsAt resolves all preload needs at the given nextIndex.
// Each attribute type uses independent caches and loaders, so they are resolved
// in parallel to reduce wall-clock time and shard lock hold duration.
func (p *Preloader) buildPreloadsAt(nextIndex uint64, snap cache.ConfigSnapshot, needs *Needs) (*raftcmdpb.PreloadSet, *CleanupToken, error) {
	boundary := cache.BoundaryIndex(nextIndex, snap.GenerationThreshold)

	if p.logger.Enabled(logging.DebugLevel) {
		p.logger.WithFields(map[string]any{
			"nextIndex":           nextIndex,
			"boundary":            boundary,
			"generationThreshold": snap.GenerationThreshold,
			"gen":                 cache.Gen(nextIndex, snap.GenerationThreshold),
		}).Debugf("Preloader: buildPreloadsAt")
	}

	// Each goroutine writes to a distinct results[slot].
	const maxTypes = 12
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
			r, results[i].err = resolveAttributePreload(
				needs.Ledgers, nextIndex, boundary,
				p.cache.Ledgers, p.loaders.Ledgers,
				p.attrs.Ledger.Get, p.store,
				buildLedgerPreload, false,
				dal.SubAttrLedger, nil,
				p.bloomFilter(dal.SubAttrLedger),
				p.logger, "ledgers",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.Ledgers
		})
	}

	if len(needs.Boundaries) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.Boundaries, nextIndex, boundary,
				p.cache.Boundaries, p.loaders.Boundaries,
				p.attrs.Boundary.Get, p.store,
				buildBoundaryPreload, false,
				dal.SubAttrBoundary, nil,
				p.bloomFilter(dal.SubAttrBoundary),
				p.logger, "boundaries",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.Boundaries
		})
	}

	if len(needs.Volumes) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.Volumes, nextIndex, boundary,
				p.cache.Volumes, p.loaders.Volumes,
				p.attrs.Volume.Get, p.store,
				buildVolumePreload, true,
				dal.SubAttrVolume, nil,
				p.bloomFilter(dal.SubAttrVolume),
				p.logger, "volumes",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.Volumes
		})
	}

	if len(needs.IdempotencyKeys) > 0 {
		launch(func(i int) {
			reader, err := p.store.NewReadHandle()
			if err != nil {
				results[i].err = err

				return
			}
			defer func() { _ = reader.Close() }()

			var preloads []*raftcmdpb.Preload
			for ik := range needs.IdempotencyKeys {
				value, err := state.LoadIdempotencyKey(reader, ik.Key)
				if err != nil {
					results[i].err = err

					return
				}

				if value != nil {
					preloads = append(preloads, &raftcmdpb.Preload{
						Type: &raftcmdpb.Preload_IdempotencyKey{
							IdempotencyKey: &raftcmdpb.PreloadIdempotencyKey{
								Key:   ik.Key,
								Value: value,
							},
						},
					})
				}
			}
			results[i].resolve = &resolveResult{preloads: preloads}
		})
	}

	if len(needs.References) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.References, nextIndex, boundary,
				p.cache.References, p.loaders.References,
				p.attrs.References.Get, p.store,
				buildReferencePreload, false,
				dal.SubAttrReference, nil,
				p.bloomFilter(dal.SubAttrReference),
				p.logger, "references",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.References
		})
	}

	if len(needs.SinkConfigs) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.SinkConfigs, nextIndex, boundary,
				p.cache.SinkConfigs, p.loaders.SinkConfigs,
				p.attrs.SinkConfig.Get, p.store,
				buildSinkConfigPreload, false,
				dal.SubAttrSinkConfig, nil,
				p.bloomFilter(dal.SubAttrSinkConfig),
				p.logger, "sink_configs",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.SinkConfigs
		})
	}

	if len(needs.NumscriptVersions) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.NumscriptVersions, nextIndex, boundary,
				p.cache.NumscriptVersions, p.loaders.NumscriptVersions,
				p.attrs.NumscriptVersion.Get, p.store,
				buildNumscriptVersionPreload, true,
				dal.SubAttrNumscriptVersion, nil,
				p.bloomFilter(dal.SubAttrNumscriptVersion),
				p.logger, "numscript_versions",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.NumscriptVersions
		})
	}

	if len(needs.NumscriptContents) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.NumscriptContents, nextIndex, boundary,
				p.cache.NumscriptContents, p.loaders.NumscriptContents,
				p.attrs.NumscriptContent.Get, p.store,
				buildNumscriptContentPreload, true,
				dal.SubAttrNumscriptContent, nil,
				p.bloomFilter(dal.SubAttrNumscriptContent),
				p.logger, "numscript_contents",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.NumscriptContents
		})
	}

	if len(needs.Transactions) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.Transactions, nextIndex, boundary,
				p.cache.Transactions, p.loaders.Transactions,
				p.attrs.Transaction.Get, p.store,
				buildTransactionStatePreload, false,
				dal.SubAttrTransaction, nil,
				p.bloomFilter(dal.SubAttrTransaction),
				p.logger, "transactions",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.Transactions
		})
	}

	if len(needs.Metadata) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.Metadata, nextIndex, boundary,
				p.cache.AccountMetadata, p.loaders.AccountMetadata,
				p.attrs.Metadata.Get, p.store,
				buildMetadataPreload, false,
				dal.SubAttrMetadata, nil,
				p.bloomFilter(dal.SubAttrMetadata),
				p.logger, "metadata",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.AccountMetadata
		})
	}

	if len(needs.PreparedQueries) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.PreparedQueries, nextIndex, boundary,
				p.cache.PreparedQueries, p.loaders.PreparedQueries,
				p.attrs.PreparedQuery.Get, p.store,
				buildPreparedQueryPreload, true,
				dal.SubAttrPreparedQuery, nil,
				p.bloomFilter(dal.SubAttrPreparedQuery),
				p.logger, "prepared_queries",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.PreparedQueries
		})
	}

	if len(needs.LedgerMetadata) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.LedgerMetadata, nextIndex, boundary,
				p.cache.LedgerMetadata, p.loaders.LedgerMetadata,
				p.attrs.LedgerMetadata.Get, p.store,
				buildLedgerMetadataPreload, false,
				dal.SubAttrLedgerMetadata, nil,
				p.bloomFilter(dal.SubAttrLedgerMetadata),
				p.logger, "ledger_metadata",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.LedgerMetadata
		})
	}

	wg.Wait()

	// Build CleanupToken and merge results, returning first error encountered.
	token := &CleanupToken{}
	preloadSet := &raftcmdpb.PreloadSet{
		LastPersistedIndex: boundary,
		CacheEpoch:         snap.Epoch,
	}

	for i := range slot {
		if results[i].err != nil {
			return nil, token, results[i].err
		}

		if results[i].resolve != nil {
			preloadSet.Preloads = append(preloadSet.Preloads, results[i].resolve.preloads...)
			preloadSet.Touches = append(preloadSet.Touches, results[i].resolve.touches...)

			if results[i].loader != nil && len(results[i].resolve.tracker) > 0 {
				token.tracked = append(token.tracked, trackedLoader{
					loader: results[i].loader,
					keys:   results[i].resolve.tracker,
				})
			}
		}
	}

	return preloadSet, token, nil
}
