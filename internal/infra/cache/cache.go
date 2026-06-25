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

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
// with per-shard RWMutex). Readers (Get, IsGuaranteedInCache) take a shared
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

// Del marks the entry as a tombstone in both Gen0 and Gen1 instead of
// removing it. This prevents pipelined MirrorTouch from failing when
// the leader admitted a touch before the FSM processed the deletion.
// Tombstones age out naturally via rotation.
func (a *AttributeCache[T]) Del(k attributes.U128) {
	if entry, ok := a.gen0.Load().Get(k); ok {
		entry.Deleted = true
		a.gen0.Load().Put(k, entry)
	}

	if entry, ok := a.gen1.Load().Get(k); ok {
		entry.Deleted = true
		a.gen1.Load().Put(k, entry)
	}
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
	// CacheGuaranteed means the key is in cache and will survive until the target index.
	CacheGuaranteed CacheStatus = iota
	// CacheNeedsTouch means the key is in Gen1 but not Gen0 — a touch (Gen1→Gen0 promotion)
	// will keep it alive without a store read.
	CacheNeedsTouch
	// CacheMiss means the key is not in cache at all — a full preload from store is needed.
	CacheMiss
)

// CheckCache determines whether a key will survive in cache until the future raft
// index `at`, needs a touch (promotion from Gen1 to Gen0), or is a full miss.
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
	threshold := a.Cache.GenerationThreshold()
	if threshold == 0 {
		return CacheMiss
	}

	a.Cache.mu.RLock()
	defer a.Cache.mu.RUnlock()

	actualGeneration := a.Cache.currentGeneration.Load()
	futureGeneration := Gen(at, threshold)

	switch futureGeneration - actualGeneration {
	case 0:
		// Same generation — no rotation expected.
		if _, ok := a.gen0.Load().Get(k); ok {
			return CacheGuaranteed
		}

		if _, ok := a.gen1.Load().Get(k); ok {
			// Key in Gen1 but not Gen0 — will survive this generation,
			// but needs a touch to survive the next rotation.
			return CacheNeedsTouch
		}

		return CacheMiss
	case 1:
		// Next generation — one rotation expected.
		// Only Gen0 data survives (it becomes Gen1 after rotation).
		if _, ok := a.gen0.Load().Get(k); ok {
			return CacheGuaranteed
		}

		// A touch is NOT safe here: the FSM applies rotation BEFORE touches,
		// so Gen1 would be discarded before the touch can promote the key.

		return CacheMiss
	default:
		// 2+ generations ahead — data will be lost regardless.
		return CacheMiss
	}
}

// IsGuaranteedInCache checks if a key will still be in the cache when we reach
// the future raft index `at`. Convenience wrapper around CheckCache.
func (a *AttributeCache[T]) IsGuaranteedInCache(at uint64, k attributes.U128) bool {
	return a.CheckCache(at, k) == CacheGuaranteed
}

// Touch promotes a key from Gen1 to Gen0. This ensures the key survives
// the next rotation without needing a full preload from store.
// Called only from the FSM goroutine (same as Put/Del).
//
// IMPORTANT: Touch must NOT overwrite an existing Gen0 entry. After rotation,
// entries processed earlier in the same batch may have already updated Gen0
// with a newer value via Merge. If a later entry's proposal was admitted
// concurrently (before the FSM applied the earlier entry's Touch+Merge),
// it would carry a redundant CacheNeedsTouch. Blindly overwriting Gen0 with
// the stale Gen1 value would discard the earlier entry's update, causing
// volume corruption (lost deltas).
func (a *AttributeCache[T]) Touch(k attributes.U128) {
	gen0 := a.gen0.Load()
	if _, ok := gen0.Get(k); ok {
		return // Gen0 already has a (possibly newer) value — do not overwrite.
	}

	if v, ok := a.gen1.Load().Get(k); ok {
		gen0.Put(k, v)
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
	Touch(attributes.U128)
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
	// touchMap maps attribute code bytes to their cache for Touch dispatch.
	touchMap map[byte]CacheOps

	// currentGeneration is accessed atomically from IsGuaranteedInCache (hot path)
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

// ResetWithThreshold atomically resets the cache, increments the epoch, and
// sets a new generation threshold. Called by the FSM when a cluster config
// change is applied — the epoch increment is deterministic (all nodes apply
// the same Raft entry).
func (c *Cache) ResetWithThreshold(threshold uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearLocked()
	c.epoch.Add(1)
	c.generationThreshold.Store(threshold)
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
	threshold := c.generationThreshold.Load()
	if threshold == 0 {
		return false, 0
	}

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

// TouchByType promotes a key from Gen1 to Gen0 for the cache identified by the
// given attribute code byte. This is the unified dispatch point used by the FSM
// Preload path instead of a per-type switch.
func (c *Cache) TouchByType(attrType byte, id attributes.U128) {
	if tc, ok := c.touchMap[attrType]; ok {
		tc.Touch(id)
	}
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
func (c *Cache) SetGenerationThreshold(v uint64) {
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

	// Register touch dispatch map for attribute code byte -> cache.
	ret.touchMap = map[byte]CacheOps{
		dal.SubAttrVolume:           ret.Volumes,
		dal.SubAttrReference:        ret.References,
		dal.SubAttrLedger:           ret.Ledgers,
		dal.SubAttrBoundary:         ret.Boundaries,
		dal.SubAttrSinkConfig:       ret.SinkConfigs,
		dal.SubAttrMetadata:         ret.AccountMetadata,
		dal.SubAttrNumscriptVersion: ret.NumscriptVersions,
		dal.SubAttrTransaction:      ret.Transactions,
		dal.SubAttrNumscriptContent: ret.NumscriptContents,
		dal.SubAttrPreparedQuery:    ret.PreparedQueries,
		dal.SubAttrLedgerMetadata:   ret.LedgerMetadata,
		dal.SubAttrIndex:            ret.Indexes,
	}

	err := ret.initMetrics(m)
	if err != nil {
		return nil, fmt.Errorf("initializing cache metrics: %w", err)
	}

	return ret, nil
}
