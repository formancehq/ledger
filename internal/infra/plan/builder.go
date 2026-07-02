package plan

import (
	"fmt"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Builder manages the shared preload infrastructure used by both admission
// and mirror. It uses the Node's IndexTracker for accurate Raft index prediction
// and the preload.Loaders for deduplication.
//
// The proposal lock is the IndexTracker's own mutex (tracker.Lock/Unlock),
// which also serializes with IndexTracker.Decrement. This prevents a race
// where a dropped proposal shifts the tracker across a generation boundary
// between AcquireProposalGuard's validation and Node.Propose.
type Builder struct {
	tracker      *node.IndexTracker
	cache        *cache.Cache
	attrs        *attributes.Attributes
	store        *dal.Store
	loaders      *preload.Loaders
	bloomFilters *bloom.FilterSet
	logger       logging.Logger

	// resolvers[attrCode] holds the per-attribute-cache resolve pipeline
	// (cache, loader, getValue, bloom, typeName). Populated once at
	// NewBuilder; buildPreloadsAt iterates over needs.Attributes and
	// dispatches through this map. Adding a new attribute cache is a
	// single-line change in buildAttrResolvers.
	resolvers map[byte]attrResolver

	// maxPlanSize is the cap on the number of AttributeCoverage entries an
	// ExecutionPlan may carry. Build returns domain.ErrExecutionPlanTooLarge
	// past this threshold so admission rejects pathological payloads up
	// front rather than paying for proportionally-large coverage slices
	// on every NewScope downstream. 0 disables the cap.
	maxPlanSize int
}

// BuildResult carries the result of an optimistic Build call.
// The caller uses ExecutionPlan for marshalling outside the lock, then passes
// the build to AcquireProposalGuard for generation validation.
//
// operations is captured here so Run can iterate them right before each
// (re-)marshal without the caller re-passing the slice. aggregate is the
// merged Coverage across all operations — used for preload boundary
// validation under the guard.
type BuildResult struct {
	ExecutionPlan *raftcmdpb.ExecutionPlan
	token         *preload.CleanupToken
	nextIndex     uint64
	nextIndexGen  uint64 // gen(nextIndex, threshold) — the future generation used by CheckCache
	operations    []WriteOperation
	aggregate     *Coverage
}

// ReleaseLoaders releases the loader cleanup token from the build.
// Use this on error paths when AcquireProposalGuard was never called.
// Safe to call multiple times (idempotent via nil check).
func (b *BuildResult) ReleaseLoaders() {
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
	p     *Builder
	token *preload.CleanupToken
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

// NewBuilder creates a Builder using the given IndexTracker for Raft index
// prediction. maxPlanSize caps the number of AttributeCoverage entries an
// ExecutionPlan may carry (0 = unlimited); see Builder.maxPlanSize.
func NewBuilder(tracker *node.IndexTracker, c *cache.Cache, attrs *attributes.Attributes, store *dal.Store, bloomFilters *bloom.FilterSet, logger logging.Logger, maxPlanSize int) *Builder {
	b := &Builder{
		tracker:      tracker,
		cache:        c,
		attrs:        attrs,
		store:        store,
		loaders:      preload.NewLoaders(),
		bloomFilters: bloomFilters,
		logger:       logger,
		maxPlanSize:  maxPlanSize,
	}
	b.resolvers = buildAttrResolvers(c, attrs, b.loaders, b.bloomFilter)

	return b
}

// Loaders returns the shared preload.Loaders instance, allowing callers to
// release tokens from a BuildResult on error paths.
func (p *Builder) Loaders() *preload.Loaders {
	return p.loaders
}

// ReadBoundaries reads the current LedgerBoundaries for the given ledger
// directly from Pebble (not from the cache overlay).
func (p *Builder) ReadBoundaries(ledgerName string) (*raftcmdpb.LedgerBoundaries, error) {
	reader, err := p.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = reader.Close() }()

	return p.attrs.Boundary.Get(reader, []byte(ledgerName))
}

// TrackerNext returns the predicted next Raft index from the IndexTracker.
func (p *Builder) TrackerNext() uint64 { return p.tracker.Next() }

// LockTracker acquires the IndexTracker's mutex. Used by non-guard callers
// (mirror error reporting, admission barrier) that Propose without going
// through AcquireProposalGuard, to serialize the tracker Increment with
// guarded proposals.
func (p *Builder) LockTracker() { p.tracker.Lock() }

// UnlockTracker releases the IndexTracker's mutex.
func (p *Builder) UnlockTracker() { p.tracker.Unlock() }

// Build resolves all preload needs optimistically without holding the
// proposal lock. operations contribute their per-operation Coverage to a
// single aggregate that drives the preload; the slice is retained on
// the BuildResult so Run can assign each operation's coverage_bits at
// marshal time.
//
// On error, the caller must call build.ReleaseLoaders().
func (p *Builder) Build(operations []WriteOperation) (*BuildResult, error) {
	aggregate := NewCoverage()
	for _, op := range operations {
		if op.Coverage != nil {
			aggregate.Merge(op.Coverage)
		}
	}

	nextIndex := p.tracker.Next()
	snap := p.cache.Snapshot()
	nextIndexGen := cache.Gen(nextIndex, snap.GenerationThreshold)

	executionPlan, token, err := p.buildPreloadsAt(nextIndex, snap, aggregate)
	if err != nil {
		return &BuildResult{token: token, nextIndex: nextIndex, nextIndexGen: nextIndexGen, operations: operations, aggregate: aggregate}, err
	}

	if p.maxPlanSize > 0 {
		if size := len(executionPlan.GetAttributes()); size > p.maxPlanSize {
			return &BuildResult{token: token, nextIndex: nextIndex, nextIndexGen: nextIndexGen, operations: operations, aggregate: aggregate},
				&domain.ErrExecutionPlanTooLarge{Size: size, Limit: p.maxPlanSize}
		}
	}

	return &BuildResult{
		ExecutionPlan: executionPlan,
		token:         token,
		nextIndex:     nextIndex,
		nextIndexGen:  nextIndexGen,
		operations:    operations,
		aggregate:     aggregate,
	}, nil
}

// AcquireProposalGuard acquires the proposal lock and validates that
// gen(nextIndex) has not changed since the optimistic Build call.
//
// CheckCache uses gen(nextIndex) as the future generation to decide whether
// cached data will survive. Under the proposal lock, nextIndex is stable
// (no other goroutine can Propose), so gen(nextIndex) is deterministic.
// The cache's currentGeneration (FSM side) is NOT checked here — it can
// advance at any time and is read atomically by CheckCache itself.
//
// Returns (updatedPreloadSet, guard, error):
//   - updatedPreloadSet is nil when stable (common case) — use build.ExecutionPlan.
//   - updatedPreloadSet is non-nil when stale (rare) — re-marshal with it.
//
// On error, the caller must call guard.ReleaseAll() if guard is non-nil.
func (p *Builder) AcquireProposalGuard(build *BuildResult) (*raftcmdpb.ExecutionPlan, *ProposalGuard, error) {
	p.tracker.Lock()

	snap := p.cache.Snapshot()
	nextIndexAfter := p.tracker.Next()
	nextIndexGenAfter := cache.Gen(nextIndexAfter, snap.GenerationThreshold)

	if build.nextIndexGen == nextIndexGenAfter {
		return nil, &ProposalGuard{p: p, token: build.token}, nil
	}

	// --- Stale: re-build under lock ---
	// nextIndex crossed a generation boundary since Build, so the
	// CheckCache decisions (hit/miss/touch) may be wrong. Under the tracker
	// lock, nextIndex is stable for the duration of the rebuild (Decrement
	// also acquires this lock, preventing the race).
	build.token.Release()

	if p.logger.Enabled(logging.TraceLevel) {
		p.logger.WithFields(map[string]any{
			"nextIndex_before":    build.nextIndex,
			"nextIndex_after":     nextIndexAfter,
			"nextIndexGen_before": build.nextIndexGen,
			"nextIndexGen_after":  nextIndexGenAfter,
		}).Tracef("nextIndex generation changed, rebuilding preloads under lock")
	}

	assert.Reachable("preloads rebuilt under proposal guard after generation crossing", map[string]any{
		"nextIndexBefore":    build.nextIndex,
		"nextIndexAfter":     nextIndexAfter,
		"nextIndexGenBefore": build.nextIndexGen,
		"nextIndexGenAfter":  nextIndexGenAfter,
	})

	executionPlan, token, err := p.buildPreloadsAt(p.tracker.Next(), snap, build.aggregate)
	if err != nil {
		// Keep the tracker lock held: we return a non-nil guard so the
		// caller can `guard.ReleaseAll()` to unlock and release the
		// loader token atomically. Unlocking here would race with that
		// `ReleaseAll()` and panic with "sync: unlock of unlocked
		// mutex" on the second call.
		return nil, &ProposalGuard{p: p, token: token}, err
	}

	return executionPlan, &ProposalGuard{p: p, token: token}, nil
}

// bloomFilter returns the bloom filter for the given attribute type, or nil
// if disabled or the bloom set is not yet ready. Readiness and the filter
// pointer are read from a single Snapshot so we never observe ready=true
// from one revision paired with a freshly-swapped empty snapshot from the
// next — that race injected zero-volume preloads after a bloom config
// change (#317).
func (p *Builder) bloomFilter(attrType byte) *bloom.Filter {
	if p.bloomFilters == nil {
		return nil
	}

	snap := p.bloomFilters.Snapshot()
	if !snap.Ready() {
		return nil
	}

	return snap.FilterForAttrType(attrType)
}

// buildResult holds the output of a single attribute-type resolution goroutine.
type buildResult struct {
	resolve *resolveResult
	err     error
	loader  preload.LoaderOps // which loader the tracker keys should be released from
}

// buildPreloadsAt resolves all preload needs at the given nextIndex.
// Each attribute type uses independent caches and loaders, so they are resolved
// in parallel to reduce wall-clock time and shard lock hold duration.
func (p *Builder) buildPreloadsAt(nextIndex uint64, snap cache.ConfigSnapshot, needs *Coverage) (*raftcmdpb.ExecutionPlan, *preload.CleanupToken, error) {
	boundary := cache.BoundaryIndex(nextIndex, snap.GenerationThreshold)

	if p.logger.Enabled(logging.TraceLevel) {
		p.logger.WithFields(map[string]any{
			"nextIndex":           nextIndex,
			"boundary":            boundary,
			"generationThreshold": snap.GenerationThreshold,
			"gen":                 cache.Gen(nextIndex, snap.GenerationThreshold),
		}).Tracef("Builder: buildPreloadsAt")
	}

	// Pre-count active attribute caches so results[] can be
	// fixed-size: each goroutine writes to a distinct index without
	// any synchronization on the slice header.
	activeAttrCodes := make([]byte, 0, len(needs.Attributes))
	for attrCode, set := range needs.Attributes {
		if len(set) == 0 {
			continue
		}

		activeAttrCodes = append(activeAttrCodes, attrCode)
	}

	// Pre-validate every attrCode has a resolver BEFORE spawning any
	// goroutine. Bailing mid-loop after a `launch` call would leave
	// in-flight goroutines writing to results[] on a slice the caller
	// no longer sees AND leak their `loader.LoadOrWait` inflight-map
	// entries (no CleanupToken assembled → the loader keeps them
	// pinned forever). Validating first makes the error path leak-free.
	for _, attrCode := range activeAttrCodes {
		if _, ok := p.resolvers[attrCode]; !ok {
			// Admission emitted a preload for a cache the builder
			// wasn't told about (new attrCode landed in dal.SubAttr*
			// without a matching entry in buildAttrResolvers).
			// Silent no-op would leave the FSM without seeded values
			// on the apply side, violating invariant #6.
			assert.Unreachable("plan builder: no resolver registered for attrCode — extend buildAttrResolvers", map[string]any{
				"attrCode": attrCode,
				"keys":     len(needs.Attributes[attrCode]),
			})

			return nil, nil, fmt.Errorf("plan builder: no resolver for attrCode 0x%x", attrCode)
		}
	}

	slotCount := len(activeAttrCodes)
	if len(needs.IdempotencyKeys) > 0 {
		slotCount++
	}

	results := make([]buildResult, slotCount)

	var wg sync.WaitGroup
	nextSlot := 0
	launch := func(fn func(i int)) {
		i := nextSlot
		nextSlot++

		wg.Go(func() {
			fn(i)
		})
	}

	for _, attrCode := range activeAttrCodes {
		set := needs.Attributes[attrCode]
		resolver := p.resolvers[attrCode] // pre-validated non-nil above

		launch(func(i int) {
			r, err := resolver.Resolve(
				set,
				nextIndex, boundary, snap.Epoch,
				p.store, p.logger,
			)
			results[i].resolve = r
			results[i].err = err
			results[i].loader = resolver.Loader()
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

			var keys []*raftcmdpb.ReloadIdempotencyKey
			for ik := range needs.IdempotencyKeys {
				value, err := state.LoadIdempotencyKey(reader, ik.Key)
				if err != nil {
					results[i].err = err

					return
				}

				if value != nil {
					keys = append(keys, &raftcmdpb.ReloadIdempotencyKey{
						Key:   ik.Key,
						Value: value,
					})
				}
			}
			results[i].resolve = &resolveResult{idempotencyKeys: keys}
		})
	}

	wg.Wait()

	// Build preload.CleanupToken and merge results, returning first error encountered.
	token := &preload.CleanupToken{}
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: boundary,
		CacheEpoch:         snap.Epoch,
	}

	for i := range results {
		// Always promote any tracker entries into the cleanup token,
		// even when the slot returned an error. resolveCoverage
		// can populate the tracker for keys that loaded successfully
		// before a concurrent sibling set firstErr; if we returned
		// before draining them, the caller's ReleaseLoaders would skip
		// them and the loader would leak the partial-load entries
		// across every retry.
		if results[i].resolve != nil && results[i].loader != nil && len(results[i].resolve.tracker) > 0 {
			token.Tracked = append(token.Tracked, preload.TrackedLoader{
				Loader: results[i].loader,
				Keys:   results[i].resolve.tracker,
			})
		}

		if results[i].err != nil {
			return nil, token, results[i].err
		}

		if results[i].resolve != nil {
			executionPlan.Attributes = append(executionPlan.Attributes, results[i].resolve.attributes...)
			executionPlan.IdempotencyKeys = append(executionPlan.IdempotencyKeys, results[i].resolve.idempotencyKeys...)
		}
	}

	return executionPlan, token, nil
}
