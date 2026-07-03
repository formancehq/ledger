package cache

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// u128Hash extracts the low 64 bits of a U128 for shard selection.
// U128 keys are BLAKE3 hashes, so Lo is uniformly distributed.
func u128Hash(k attributes.U128) uint64 {
	return k.Lo()
}

// newShardedMap creates a ShardedMap keyed by U128 with the standard hash function.
func newShardedMap[T any]() *kv.ShardedMap[attributes.U128, attributes.Entry[T]] {
	return kv.NewShardedMap[attributes.U128, attributes.Entry[T]](u128Hash)
}

// AttributeCache is a dual-generation cache for attribute values.
// It uses atomic.Pointer for Gen0/Gen1 backed by kv.ShardedMap (64 shards
// with per-shard RWMutex). Readers (Get, Lookup, CheckCache) take a shared
// RLock on a single shard — no interface boxing, no heap allocation for U128 keys.
// Writers (Put, Del) are called only from the FSM goroutine.
// Only Rotate needs synchronization, which is done by the FSM goroutine
// under Cache.mu — no contention with admission goroutines.
type AttributeCache[T any] struct {
	gen0      atomic.Pointer[kv.ShardedMap[attributes.U128, attributes.Entry[T]]]
	gen1      atomic.Pointer[kv.ShardedMap[attributes.U128, attributes.Entry[T]]]
	Cache     *Cache
	cacheType string
}

func (a *AttributeCache[T]) Gen0() *kv.ShardedMap[attributes.U128, attributes.Entry[T]] {
	return a.gen0.Load()
}

func (a *AttributeCache[T]) Gen1() *kv.ShardedMap[attributes.U128, attributes.Entry[T]] {
	return a.gen1.Load()
}

// Get returns the entry for k, falling back to Gen1 when Gen0 misses.
//
// The gen0 → gen1 fallback is safe under the FSM's coverage gate
// (invariant #9): every read the apply path performs goes through
// Scope.GetX with an admission-declared preload, so the fallback can
// only surface keys the proposer explicitly authorized. Reads on
// undeclared keys are rejected at the gate before ever reaching Get.
//
// Callers that need to distinguish Gen0 from Gen1 explicitly (e.g.
// MirrorPreload's gen1-wins seed decision, snapshot persistence) use the
// Gen0() / Gen1() accessors.
func (a *AttributeCache[T]) Get(k attributes.U128) (attributes.Entry[T], bool) {
	if v, ok := a.gen0.Load().Get(k); ok {
		return v, true
	}

	return a.gen1.Load().Get(k)
}

func (a *AttributeCache[T]) Put(k attributes.U128, v attributes.Entry[T]) {
	a.gen0.Load().Put(k, v)
}

func (a *AttributeCache[T]) GetAndPut(k attributes.U128, v attributes.Entry[T]) (attributes.Entry[T], bool) {
	old, existed := a.gen0.Load().GetAndPut(k, v)
	if existed {
		return old, true
	}

	// New value is already in gen0. Check gen1 for the old value.
	return a.gen1.Load().Get(k)
}

// Del tombstones the entry in Gen0 in-place, mirroring the on-disk
// writeCacheTombstone which writes a single row to the current gen0 byte.
//
// If Gen0 doesn't hold the entry, Del promotes the Gen1 entry's tag into
// a fresh Gen0 tombstone (Deleted=true, Data=zero). Gen1's live row is
// intentionally left untouched: the Gen0 tombstone shadows it on every
// read (Get returns the tombstone via the gen0→gen1 fallback semantics —
// gen0 hits first), and rotation purges the stale Gen1 row on the next
// generation flip. This lazy promote replaces the historical
// systematic-MirrorTouch-at-Preload pass: coverage_bits (invariant #9)
// already prevents Del from firing on keys admission did not declare,
// so promoting at Del time is both sufficient and cheaper than
// pre-promoting every declared key upfront.
//
// Returns ErrNotFound if the key is genuinely absent from both
// generations. Under proper admission, every Delete is preceded by a
// Get that would surface the same ErrNotFound at the business layer,
// so this error rarely reaches Del — DerivedKeyStore.Merge treats it
// as a soft no-op.
//
// Data is reset to the zero value: a tombstone's payload is unreadable
// by contract (every consumer checks Deleted first), and retaining it
// has historically caused snapshot/restore resurrection (EN-1377).
//
// Called only from the FSM goroutine.
func (a *AttributeCache[T]) Del(k attributes.U128) error {
	var zero T

	gen0 := a.gen0.Load()
	if entry, ok := gen0.Get(k); ok {
		entry.Deleted = true
		entry.Data = zero
		gen0.Put(k, entry)

		return nil
	}

	// Gen0 miss — try Gen1 to reuse the entry's tag on the fabricated
	// tombstone. The tag is what KeyStore.Delete matched against; a
	// U128 collision on a different canonical key would already have
	// been rejected upstream by KeyStore.Delete's tag check.
	if gen1Entry, ok := a.gen1.Load().Get(k); ok {
		gen0.Put(k, attributes.Entry[T]{Tag: gen1Entry.Tag, Deleted: true, Data: zero})

		return nil
	}

	return domain.ErrNotFound
}

func (a *AttributeCache[T]) Size() uint64 {
	return a.gen0.Load().Size() + a.gen1.Load().Size()
}

func (a *AttributeCache[T]) Iter() iter.Seq2[attributes.U128, attributes.Entry[T]] {
	return func(yield func(attributes.U128, attributes.Entry[T]) bool) {
		for k, v := range a.gen0.Load().Iter() {
			if !yield(k, v) {
				return
			}
		}

		for k, v := range a.gen1.Load().Iter() {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Rotate performs a generation rotation: Gen1 is replaced by Gen0,
// and Gen0 is replaced by a new empty ShardedMap.
// Called only from the FSM goroutine under Cache.mu.
func (a *AttributeCache[T]) Rotate() {
	old := a.gen0.Load()
	a.gen1.Store(old)
	a.gen0.Store(newShardedMap[T]())
}

// Reset clears all data in both generations.
func (a *AttributeCache[T]) Reset() {
	a.gen0.Store(newShardedMap[T]())
	a.gen1.Store(newShardedMap[T]())
}

// CacheStatus describes the result of checking a key against the dual-generation cache.
type CacheStatus int

const (
	// CacheHit means the key is in cache somewhere within the reachable
	// horizon (Gen0 hit now, or Gen1 hit that AttributeCache.Get's gen0→gen1
	// fallback will surface at apply). No Pebble read is required. The
	// coverage_bits gate (invariant #9) bounds the FSM's read horizon to
	// admission's declared preload set, so the fallback can only reach
	// keys the proposer explicitly authorized. Admission therefore does
	// not need to distinguish "already in Gen0" from "Gen1-only".
	CacheHit CacheStatus = iota
	// CacheMiss means the key is not in cache at all — a full preload
	// from store is needed.
	CacheMiss
	// CacheUnreachable means the target index is 2+ generations ahead of the
	// current FSM-applied generation: any preload value computed now would be
	// invalidated by the rotations that must run before apply (gen0 -> gen1 ->
	// discarded). The admission cannot guarantee that the proposal will read a
	// consistent cache horizon, so the order must be rejected and re-admitted
	// once the FSM apply has caught up. This is an operational signal — under
	// a correctly tuned rotation threshold and a healthy apply rate it should
	// not occur.
	CacheUnreachable
)

// CheckCache determines whether a key will survive in cache until the future raft
// index `at`, or is a full miss.
//
// The cache uses a dual-generation system where:
// - Gen0 contains data from the current generation
// - Gen1 contains data from the previous generation
// - On rotation: Gen1 is discarded, Gen0 becomes Gen1, new Gen0 is created
//
// The `at` parameter comes from the Node's IndexTracker, which accurately tracks
// all Raft index consumers (proposals, no-ops, config changes). This ensures the
// predicted generation matches the actual Raft index progression.
//
// This method only takes a brief shared RLock on a single shard.
// CheckCache determines whether a key will survive in cache until the future
// raft index `at`. Takes a read lock on the cache to ensure a consistent view
// of currentGeneration and the gen0/gen1 data during the check.
func (a *AttributeCache[T]) CheckCache(at uint64, k attributes.U128) CacheStatus {
	a.Cache.mu.RLock()
	defer a.Cache.mu.RUnlock()

	// Threshold + currentGeneration are read INSIDE the RLock so a concurrent
	// ResetWithThreshold (write-lock holder) cannot bump one between our
	// reads and leave us with a (threshold=old, currentGeneration=new)
	// snapshot. Such a torn view would classify a valid admission as
	// CacheUnreachable during the threshold-change transition window.
	//
	// threshold > 0 is a cluster-wide invariant: cache.New rejects 0 and
	// ResetWithThreshold panics on 0 — no legitimate call path can observe
	// threshold=0 here.
	threshold := a.Cache.GenerationThreshold()
	actualGeneration := a.Cache.currentGeneration.Load()
	futureGeneration := Gen(at, threshold)

	// Stale-behind build: an admission build sampled `at` before the FSM
	// applied entries past that index, so its `at` maps to a generation the
	// FSM has already left behind. Not a horizon violation — the higher-level
	// staleness guard (checkStaleProposal / AcquireProposalGuard) will
	// reject or rebuild. Report CacheMiss so the caller loads from store and
	// the concurrent apply resolves the outcome; do NOT let the uint64
	// subtraction underflow into the CacheUnreachable default branch.
	if futureGeneration < actualGeneration {
		return CacheMiss
	}

	switch futureGeneration - actualGeneration {
	case 0:
		// Same generation — no rotation expected before apply. Either
		// gen holds the key: the FSM apply path reads via Get which
		// falls back to Gen1, so the caller only needs to know "in
		// cache anywhere".
		if _, ok := a.gen0.Load().Get(k); ok {
			return CacheHit
		}

		if _, ok := a.gen1.Load().Get(k); ok {
			return CacheHit
		}

		return CacheMiss
	case 1:
		// Next generation — one rotation expected before apply. Rotation
		// moves old Gen0 into new Gen1 and empties new Gen0; old Gen1 is
		// discarded.
		//
		// If Gen0 has the key now, post-rotation it sits in new Gen1 only.
		// Get's gen0→gen1 fallback still surfaces it (and lazy Del promotes
		// on tombstone if the handler deletes).
		if _, ok := a.gen0.Load().Get(k); ok {
			return CacheHit
		}

		// If Gen0 lacks the key, the entry (if it exists) lives in old
		// Gen1 which rotation will discard — no promote can save it.
		// Fall through to CacheMiss so the caller loads from store.
		return CacheMiss
	default:
		// 2+ generations ahead — any preload value computed now would be
		// rotated out (gen0 -> gen1 -> discarded) before apply. Signal an
		// unreachable horizon so admission can reject the proposal with a
		// transient error; the client retries and admission re-admits with
		// a fresh prediction once the FSM apply has caught up.
		return CacheUnreachable
	}
}

func newAttributeCache[T any](cache *Cache, cacheType string) *AttributeCache[T] {
	ac := &AttributeCache[T]{
		Cache:     cache,
		cacheType: cacheType,
	}
	ac.gen0.Store(newShardedMap[T]())
	ac.gen1.Store(newShardedMap[T]())

	return ac
}

// CacheOps is the non-generic interface satisfied by every AttributeCache[T].
// It captures the mechanical operations that are identical for every attribute
// type, allowing Cache to iterate over all caches in a single loop instead of
// repeating per-type calls.
type CacheOps interface {
	Rotate()
	Reset()
	Size() uint64
}

type Cache struct {
	mu                  sync.RWMutex
	Volumes             *AttributeCache[*raftcmdpb.VolumePair]
	AccountMetadata     *AttributeCache[*commonpb.MetadataValue]
	References          *AttributeCache[*commonpb.TransactionReferenceValue]
	Ledgers             *AttributeCache[*commonpb.LedgerInfo]
	Boundaries          *AttributeCache[*raftcmdpb.LedgerBoundaries]
	Transactions        *AttributeCache[*commonpb.TransactionState]
	SinkConfigs         *AttributeCache[*commonpb.SinkConfig]
	NumscriptVersions   *AttributeCache[*commonpb.NumscriptVersionValue]
	NumscriptContents   *AttributeCache[*commonpb.NumscriptInfo]
	PreparedQueries     *AttributeCache[*commonpb.PreparedQuery]
	LedgerMetadata      *AttributeCache[*commonpb.MetadataValue]
	Indexes             *AttributeCache[*commonpb.Index]
	BaseIndex           DualGen[uint64]
	generationThreshold atomic.Uint64

	// caches holds all AttributeCache instances for iteration in Rotate/Reset/metrics.
	// Parallel to cacheNames.
	caches []CacheOps
	// cacheNames holds the metric label for each cache, parallel to caches.
	cacheNames []string

	// currentGeneration is accessed atomically from CheckCache (hot path)
	// and written under mu by rotateLocked/Reset.
	currentGeneration atomic.Uint64

	// epoch is incremented on every Reset(). Used by the admission layer to
	// detect that the cache was invalidated between preload building and FSM
	// application (e.g. after a cluster config change).
	epoch atomic.Uint64

	// Metrics (nil if not initialized)
	rotations       metric.Int64Counter
	generationGauge metric.Int64Gauge
	// sizeRegistration holds the callback registration for size metrics
	sizeRegistration metric.Registration
}

// rotateLocked performs the actual rotation of all cache generations.
// Must be called with c.mu held (write lock). CheckCache takes a read lock,
// ensuring it always sees a consistent snapshot of currentGeneration and
// the gen0/gen1 cache data.
func (c *Cache) rotateLocked(index uint64, newGeneration uint64) {
	c.currentGeneration.Store(newGeneration)

	for _, ac := range c.caches {
		ac.Rotate()
	}

	c.BaseIndex.Rotate(index)

	c.recordRotation()
	c.recordGeneration(int64(newGeneration))
}

// Reset clears all cache data and resets the state to initial values.
// This is used during snapshot restoration. Does NOT increment the epoch
// because this is a local rebuild, not a FSM state change — the epoch
// must be deterministic across all nodes.
func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearLocked()
}

// ResetWithThreshold atomically resets the cache, increments the epoch, sets
// a new generation threshold, AND realigns currentGeneration + BaseIndex to
// the generation that raftIndex falls into under the new threshold. Called
// by the FSM when a cluster config change is applied.
//
// The epoch increment is deterministic (all nodes apply the same Raft
// entry). Realigning currentGeneration and BaseIndex in the same critical
// section closes the race window where admission's CheckCache would
// otherwise observe currentGeneration=0 against the new threshold and
// falsely trip the CacheUnreachable horizon (2+ predicted rotations); the
// caller-provided raftIndex is the entry the FSM is applying, so
// Gen(raftIndex, threshold) is the correct post-reset horizon.
//
// A zero threshold is a config invariant violation — cache.New already
// rejects it, and every code path that reaches this method has validated
// the config upstream. Panics loudly rather than silently disabling
// rotations, which would leave currentGeneration frozen at 0 forever and
// break the CacheUnreachable / CheckRotationNeeded contracts.
//
// Callers that persist the reset to Pebble must read
// Cache.CurrentGeneration + Cache.BaseIndex.{Gen0,Gen1} after the call so
// the on-disk sentinels reflect the same in-memory state a
// RestoreFromStore would reconstruct.
func (c *Cache) ResetWithThreshold(threshold, raftIndex uint64) {
	if threshold == 0 {
		panic("cache.ResetWithThreshold: threshold must be > 0 (invariant enforced by cache.New)")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearLocked()
	c.epoch.Add(1)
	c.generationThreshold.Store(threshold)

	// clearLocked already set currentGeneration=0 and BaseIndex={0,0}. If
	// raftIndex falls inside the first generation, that's the final state.
	// Otherwise realign under the same lock.
	if g := Gen(raftIndex, threshold); g != 0 {
		boundary := genEndIndex(g-1, threshold)
		c.rotateLocked(boundary, g)
	}
}

// clearLocked clears all cache data without incrementing the epoch.
func (c *Cache) clearLocked() {
	for _, ac := range c.caches {
		ac.Reset()
	}

	c.BaseIndex = newDualGen[uint64](0, 0)
	c.currentGeneration.Store(0)
}

// CheckRotationNeeded checks if a generation rotation is needed for the given index
// and performs it atomically if necessary.
// Returns whether a rotation occurred and the old Gen1 base index (compaction threshold).
func (c *Cache) CheckRotationNeeded(index uint64) (rotated bool, oldGen1BaseIndex uint64) {
	// threshold > 0 is a cluster-wide invariant (see CheckCache / New /
	// ResetWithThreshold) — no dead-code guard here.
	threshold := c.generationThreshold.Load()

	c.mu.Lock()
	defer c.mu.Unlock()

	if g := Gen(index, threshold); g != c.currentGeneration.Load() {
		oldGen1BaseIndex = c.BaseIndex.Gen1
		// Use canonical boundary (end of previous generation) instead of raw index.
		// This must match BoundaryIndex(nextIndex, K) used by admission when building preloads.
		boundary := genEndIndex(g-1, threshold)
		c.rotateLocked(boundary, g)

		return true, oldGen1BaseIndex
	}

	return false, 0
}

// initMetrics initializes the cache metrics on the Cache.
func (c *Cache) initMetrics(m metric.Meter) error {
	rotations, err := m.Int64Counter(
		"cache.rotations",
		metric.WithDescription("Number of cache generation rotations"),
	)
	if err != nil {
		return err
	}

	generation, err := m.Int64Gauge(
		"cache.generation",
		metric.WithDescription("Current cache generation number"),
	)
	if err != nil {
		return err
	}

	// Create observable gauge for cache sizes
	sizeGauge, err := m.Int64ObservableGauge(
		"cache.size",
		metric.WithDescription("Number of entries in the cache"),
	)
	if err != nil {
		return err
	}

	// Register callback to observe cache sizes using the caches/cacheNames slices.
	allCaches := c.caches
	allNames := c.cacheNames

	registration, err := m.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			for i, ac := range allCaches {
				o.ObserveInt64(sizeGauge, int64(ac.Size()),
					metric.WithAttributes(attribute.String("type", allNames[i])))
			}

			return nil
		},
		sizeGauge,
	)
	if err != nil {
		return err
	}

	c.rotations = rotations
	c.generationGauge = generation
	c.sizeRegistration = registration

	return nil
}

// recordRotation records a cache generation rotation.
func (c *Cache) recordRotation() {
	if c.rotations == nil {
		return
	}

	c.rotations.Add(context.Background(), 1)
}

// recordGeneration records the current generation number.
func (c *Cache) recordGeneration(gen int64) {
	if c.generationGauge == nil {
		return
	}

	c.generationGauge.Record(context.Background(), gen)
}

// CurrentGeneration returns the current generation number.
func (c *Cache) CurrentGeneration() uint64 {
	return c.currentGeneration.Load()
}

// SetCurrentGeneration sets the current generation number (used during snapshot restore).
func (c *Cache) SetCurrentGeneration(g uint64) {
	c.currentGeneration.Store(g)
}

// GenerationThreshold returns the cache rotation threshold.
func (c *Cache) GenerationThreshold() uint64 {
	return c.generationThreshold.Load()
}

// SetGenerationThreshold updates the cache rotation threshold atomically.
// Panics on 0 — threshold > 0 is a cluster-wide invariant enforced by
// cache.New and ResetWithThreshold.
func (c *Cache) SetGenerationThreshold(v uint64) {
	if v == 0 {
		panic("cache.SetGenerationThreshold: threshold must be > 0 (invariant enforced by cache.New)")
	}
	c.generationThreshold.Store(v)
}

// Epoch returns the current cache epoch.
func (c *Cache) Epoch() uint64 {
	return c.epoch.Load()
}

// SetEpoch restores the cache epoch from persisted state.
func (c *Cache) SetEpoch(e uint64) {
	c.epoch.Store(e)
}

// ConfigSnapshot holds a consistent snapshot of mutable cache parameters.
type ConfigSnapshot struct {
	GenerationThreshold uint64
	Epoch               uint64
}

// Snapshot returns a consistent snapshot of the cache's mutable config.
// Takes the cache mutex to ensure epoch and generationThreshold are read
// atomically with respect to Reset() which updates both.
func (c *Cache) Snapshot() ConfigSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	return ConfigSnapshot{
		GenerationThreshold: c.generationThreshold.Load(),
		Epoch:               c.epoch.Load(),
	}
}

// New creates a new Cache with the given generation threshold and meter.
// If meter is nil, a noop meter is used.
func New(generationThreshold uint64, m metric.Meter) (*Cache, error) {
	if generationThreshold == 0 {
		return nil, errors.New("generation threshold must be greater than zero")
	}

	if m == nil {
		m = noop.Meter{}
	}

	ret := &Cache{
		BaseIndex: newDualGen[uint64](0, 0),
	}
	// Initialize epoch to 1 — never 0. 0 used to be both the atomic zero value
	// AND the "unset" sentinel the FSM stale-proposal check skipped on, which
	// made the check inert during a cluster's first epoch (#302). Persisted
	// values of 0 (clusters that ran before this fix) are bumped back to 1 by
	// RecoverState.
	ret.epoch.Store(1)
	ret.generationThreshold.Store(generationThreshold)
	ret.Volumes = newAttributeCache[*raftcmdpb.VolumePair](ret, "volumes")
	ret.AccountMetadata = newAttributeCache[*commonpb.MetadataValue](ret, "account_metadata")
	ret.References = newAttributeCache[*commonpb.TransactionReferenceValue](ret, "references")
	ret.Ledgers = newAttributeCache[*commonpb.LedgerInfo](ret, "ledgers")
	ret.Boundaries = newAttributeCache[*raftcmdpb.LedgerBoundaries](ret, "boundaries")
	ret.Transactions = newAttributeCache[*commonpb.TransactionState](ret, "transactions")
	ret.SinkConfigs = newAttributeCache[*commonpb.SinkConfig](ret, "sink_configs")
	ret.NumscriptVersions = newAttributeCache[*commonpb.NumscriptVersionValue](ret, "numscript_versions")
	ret.NumscriptContents = newAttributeCache[*commonpb.NumscriptInfo](ret, "numscript_contents")
	ret.PreparedQueries = newAttributeCache[*commonpb.PreparedQuery](ret, "prepared_queries")
	ret.LedgerMetadata = newAttributeCache[*commonpb.MetadataValue](ret, "ledger_metadata")
	ret.Indexes = newAttributeCache[*commonpb.Index](ret, "indexes")

	// Register all caches for iteration in Rotate/Reset/metrics.
	ret.caches = []CacheOps{
		ret.Volumes, ret.AccountMetadata,
		ret.References, ret.Ledgers, ret.Boundaries,
		ret.Transactions, ret.SinkConfigs, ret.NumscriptVersions,
		ret.NumscriptContents, ret.PreparedQueries, ret.LedgerMetadata,
		ret.Indexes,
	}
	ret.cacheNames = []string{
		"volumes", "account_metadata",
		"references", "ledgers", "boundaries",
		"transactions", "sink_configs", "numscript_versions",
		"numscript_contents", "prepared_queries", "ledger_metadata",
		"indexes",
	}

	err := ret.initMetrics(m)
	if err != nil {
		return nil, fmt.Errorf("initializing cache metrics: %w", err)
	}

	return ret, nil
}
