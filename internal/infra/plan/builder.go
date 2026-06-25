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
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

	// maxPlanSize is the cap on the number of AttributePlan entries an
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
// merged Needs across all operations — used for preload boundary
// validation under the guard.
type BuildResult struct {
	ExecutionPlan *raftcmdpb.ExecutionPlan
	token         *preload.CleanupToken
	nextIndex     uint64
	nextIndexGen  uint64 // gen(nextIndex, threshold) — the future generation used by CheckCache
	operations    []WriteOperation
	aggregate     *Needs
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
// prediction. maxPlanSize caps the number of AttributePlan entries an
// ExecutionPlan may carry (0 = unlimited); see Builder.maxPlanSize.
func NewBuilder(tracker *node.IndexTracker, c *cache.Cache, attrs *attributes.Attributes, store *dal.Store, bloomFilters *bloom.FilterSet, logger logging.Logger, maxPlanSize int) *Builder {
	return &Builder{
		tracker:      tracker,
		cache:        c,
		attrs:        attrs,
		store:        store,
		loaders:      preload.NewLoaders(),
		bloomFilters: bloomFilters,
		logger:       logger,
		maxPlanSize:  maxPlanSize,
	}
}

// Loaders returns the shared preload.Loaders instance, allowing callers to
// release tokens from a BuildResult on error paths.
func (p *Builder) Loaders() *preload.Loaders {
	return p.loaders
}

// ResolveLedgerID resolves a ledger name to its uint32 ID using the standard
// attribute resolution path: bloom → cache → Pebble.
// Returns (0, false) if the ledger does not exist.
func (p *Builder) ResolveLedgerID(name string) (uint32, bool) {
	canonical := domain.LedgerKey{Name: name}.Bytes()
	id, _ := attributes.MakeKey(canonical)

	// 1. Bloom filter: if definitely absent, skip. Use the IsReady-guarded
	// helper — the raw FilterForAttrType returns the (still-empty) filter
	// even while restoreBloomFilters / StartAsyncBloomPopulate is rebuilding
	// it on boot or after a rebuild. During that window every MayContain
	// returns false, so ResolveLedgerID would falsely answer "not found"
	// for every pre-existing ledger and admission would reject the matching
	// proposals with ErrBalanceNotPreloaded / reverts with ErrLedgerNotFound
	// (#318).
	bf := p.bloomFilter(dal.SubAttrLedger)
	if bf != nil && !bf.MayContain(id) {
		return 0, false
	}

	// bf == nil with bloom configured but not ready means the populate window
	// (#318) is open and we are falling through to cache/Pebble. Re-reading
	// readiness here can race SetReady: the window may close between the two
	// reads, which only under-reports the condition — it never over-reports
	// (a bloom-disabled deployment keeps bloomFilters == nil and a ready
	// snapshot keeps Ready() == true).
	bloomPopulating := bf == nil && p.bloomFilters != nil && !p.bloomFilters.Snapshot().Ready()

	var (
		resolvedID uint32
		resolved   bool
	)

	// 2. Cache: check gen0/gen1.
	if entry, ok := p.cache.Ledgers.Get(id); ok && entry.Data != nil {
		resolvedID, resolved = entry.Data.GetId(), true
	} else if info, err := p.attrs.Ledger.Get(p.store, canonical); err == nil && info != nil {
		// 3. Pebble fallback (single point read, no snapshot needed).
		resolvedID, resolved = info.GetId(), true
	}

	// Antithesis anchor: a pre-existing ledger was successfully resolved
	// through the fallback path while the bloom filters were still
	// populating — proves the not-ready window degrades gracefully instead
	// of faking absence.
	assert.Sometimes(bloomPopulating && resolved, "ledger resolved while bloom filters not ready", nil)

	return resolvedID, resolved
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
// proposal lock. operations contribute their per-operation Needs to a
// single aggregate that drives the preload; the slice is retained on
// the BuildResult so Run can assign each operation's coverage_bits at
// marshal time.
//
// On error, the caller must call build.ReleaseLoaders().
func (p *Builder) Build(operations []WriteOperation) (*BuildResult, error) {
	aggregate := NewNeeds()
	for _, op := range operations {
		if op.Needs != nil {
			aggregate.Merge(op.Needs)
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
func (p *Builder) buildPreloadsAt(nextIndex uint64, snap cache.ConfigSnapshot, needs *Needs) (*raftcmdpb.ExecutionPlan, *preload.CleanupToken, error) {
	boundary := cache.BoundaryIndex(nextIndex, snap.GenerationThreshold)

	if p.logger.Enabled(logging.TraceLevel) {
		p.logger.WithFields(map[string]any{
			"nextIndex":           nextIndex,
			"boundary":            boundary,
			"generationThreshold": snap.GenerationThreshold,
			"gen":                 cache.Gen(nextIndex, snap.GenerationThreshold),
		}).Tracef("Builder: buildPreloadsAt")
	}

	// Each goroutine writes to a distinct results[slot]. Bump this constant
	// whenever a new launch() call is added: with the Indexes attribute
	// (PR #453) the count went from 12 to 13, and `results[12]` would
	// otherwise panic with an out-of-range index when every category fires.
	const maxTypes = 13
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
				needs.Ledgers, nextIndex, boundary, snap.Epoch,
				p.cache.Ledgers, p.loaders.Ledgers,
				p.attrs.Ledger.Get, p.store,
				nil, false,
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
				needs.Boundaries, nextIndex, boundary, snap.Epoch,
				p.cache.Boundaries, p.loaders.Boundaries,
				p.attrs.Boundary.Get, p.store,
				nil, false,
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
				needs.Volumes, nextIndex, boundary, snap.Epoch,
				p.cache.Volumes, p.loaders.Volumes,
				p.attrs.Volume.Get, p.store,
				newZeroVolumePair, true,
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

	if len(needs.References) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.References, nextIndex, boundary, snap.Epoch,
				p.cache.References, p.loaders.References,
				p.attrs.References.Get, p.store,
				nil, false,
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
				needs.SinkConfigs, nextIndex, boundary, snap.Epoch,
				p.cache.SinkConfigs, p.loaders.SinkConfigs,
				p.attrs.SinkConfig.Get, p.store,
				nil, false,
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
				needs.NumscriptVersions, nextIndex, boundary, snap.Epoch,
				p.cache.NumscriptVersions, p.loaders.NumscriptVersions,
				p.attrs.NumscriptVersion.Get, p.store,
				nil, true,
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
				needs.NumscriptContents, nextIndex, boundary, snap.Epoch,
				p.cache.NumscriptContents, p.loaders.NumscriptContents,
				p.attrs.NumscriptContent.Get, p.store,
				nil, true,
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
				needs.Transactions, nextIndex, boundary, snap.Epoch,
				p.cache.Transactions, p.loaders.Transactions,
				p.attrs.Transaction.Get, p.store,
				nil, false,
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
				needs.Metadata, nextIndex, boundary, snap.Epoch,
				p.cache.AccountMetadata, p.loaders.AccountMetadata,
				p.attrs.Metadata.Get, p.store,
				nil, false,
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
				needs.PreparedQueries, nextIndex, boundary, snap.Epoch,
				p.cache.PreparedQueries, p.loaders.PreparedQueries,
				p.attrs.PreparedQuery.Get, p.store,
				nil, true,
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
				needs.LedgerMetadata, nextIndex, boundary, snap.Epoch,
				p.cache.LedgerMetadata, p.loaders.LedgerMetadata,
				p.attrs.LedgerMetadata.Get, p.store,
				nil, false,
				dal.SubAttrLedgerMetadata, nil,
				p.bloomFilter(dal.SubAttrLedgerMetadata),
				p.logger, "ledger_metadata",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.LedgerMetadata
		})
	}

	if len(needs.Indexes) > 0 {
		launch(func(i int) {
			var r *resolveResult
			r, results[i].err = resolveAttributePreload(
				needs.Indexes, nextIndex, boundary, snap.Epoch,
				p.cache.Indexes, p.loaders.Indexes,
				p.attrs.Index.Get, p.store,
				nil, false,
				dal.SubAttrIndex, nil,
				p.bloomFilter(dal.SubAttrIndex),
				p.logger, "indexes",
			)
			results[i].resolve = r
			results[i].loader = p.loaders.Indexes
		})
	}

	wg.Wait()

	// Build preload.CleanupToken and merge results, returning first error encountered.
	token := &preload.CleanupToken{}
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: boundary,
		CacheEpoch:         snap.Epoch,
	}

	for i := range slot {
		// Always promote any tracker entries into the cleanup token,
		// even when the slot returned an error. resolveAttributePreload
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

// newZeroVolumePair seeds the bloom-confirmed-absent volume preload with
// {Input:0, Output:0}. Other attribute kinds use a nil-zero T directly (an
// empty proto marshals to empty bytes and the FSM unmarshal restores a
// fresh zero proto), but volumes need the explicit Uint256(0) sentinels
// so postings against fresh accounts find a balance to debit/credit.
func newZeroVolumePair() *raftcmdpb.VolumePair {
	return &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
}
