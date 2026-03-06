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

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/kv"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
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

func (a *AttributeCache[T]) Del(k attributes.U128) {
	a.gen0.Load().Del(k)
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

// IsGuaranteedInCache checks if a key will still be in the cache when we reach
// the future raft index `at`. This is used to determine if cached data will survive
// until a future point in time.
//
// The cache uses a dual-generation system where:
// - Gen0 contains data from the current generation
// - Gen1 contains data from the previous generation
// - On rotation: Gen1 is discarded, Gen0 becomes Gen1, new Gen0 is created
//
// Therefore:
// - If `at` is in the current generation: no rotation, data will be there
// - If `at` is in the next generation: one rotation, data must be in Gen0 to survive
// - If `at` is 2+ generations ahead: data will be lost (too many rotations)
//
// This method only takes a brief shared RLock on a single shard.
func (a *AttributeCache[T]) IsGuaranteedInCache(at uint64, k attributes.U128) bool {
	threshold := a.Cache.GenerationThreshold // immutable after init
	if threshold == 0 {
		return false
	}

	actualGeneration := a.Cache.currentGeneration.Load()
	futureGeneration := gen(at, threshold)

	// Check how far ahead the target generation is from current
	switch futureGeneration - actualGeneration {
	case 0:
		// Same generation - no rotation will occur, data will still be there
		_, ok := a.Get(k)

		return ok
	case 1:
		// Next generation - one rotation will occur
		// Data must be in Gen0 now to survive (Gen0 becomes Gen1 after rotation)
		_, ok := a.gen0.Load().Get(k)

		return ok
	default:
		// 2+ generations ahead - too many rotations, data will be lost
		return false
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

type Cache struct {
	mu                  sync.Mutex
	Volumes             *AttributeCache[*raftcmdpb.VolumePair]
	AccountMetadata     *AttributeCache[*commonpb.MetadataValue]
	IdempotencyKeys     *AttributeCache[*commonpb.IdempotencyKeyValue]
	References          *AttributeCache[*commonpb.TransactionReferenceValue]
	Ledgers             *AttributeCache[*commonpb.LedgerInfo]
	Boundaries          *AttributeCache[*raftcmdpb.LedgerBoundaries]
	SinkConfigs         *AttributeCache[*commonpb.SinkConfig]
	NumscriptVersions   *AttributeCache[string]
	NumscriptEntries    *AttributeCache[bool]
	BaseIndex           DualGen[uint64]
	GenerationThreshold uint64

	// currentGeneration is accessed atomically from IsGuaranteedInCache (hot path)
	// and written under mu by rotateLocked/Reset.
	currentGeneration atomic.Uint64

	// Metrics (nil if not initialized)
	rotations       metric.Int64Counter
	generationGauge metric.Int64Gauge
	// sizeRegistration holds the callback registration for size metrics
	sizeRegistration metric.Registration
}

// rotateLocked performs the actual rotation of all cache generations.
// Must be called with c.mu held.
func (c *Cache) rotateLocked(index uint64, newGeneration uint64) {
	c.Volumes.Rotate()
	c.AccountMetadata.Rotate()
	c.IdempotencyKeys.Rotate()
	c.References.Rotate()
	c.Ledgers.Rotate()
	c.Boundaries.Rotate()
	c.SinkConfigs.Rotate()
	c.NumscriptVersions.Rotate()
	c.NumscriptEntries.Rotate()
	c.BaseIndex.Rotate(index)
	c.currentGeneration.Store(newGeneration)

	c.recordRotation()
	c.recordGeneration(int64(newGeneration))
}

// Reset clears all cache data and resets the state to initial values.
// This is used during snapshot restoration.
func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Volumes.Reset()
	c.AccountMetadata.Reset()
	c.IdempotencyKeys.Reset()
	c.References.Reset()
	c.Ledgers.Reset()
	c.Boundaries.Reset()
	c.SinkConfigs.Reset()
	c.NumscriptVersions.Reset()
	c.NumscriptEntries.Reset()
	c.BaseIndex = newDualGen[uint64](0, 0)
	c.currentGeneration.Store(0)
}

// CheckRotationNeeded checks if a generation rotation is needed for the given index
// and performs it atomically if necessary.
// Returns whether a rotation occurred and the old Gen1 base index (compaction threshold).
func (c *Cache) CheckRotationNeeded(index uint64) (rotated bool, oldGen1BaseIndex uint64) {
	if c.GenerationThreshold == 0 {
		return false, 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if g := gen(index, c.GenerationThreshold); g != c.currentGeneration.Load() {
		oldGen1BaseIndex = c.BaseIndex.Gen1
		// Use canonical boundary (end of previous generation) instead of raw index.
		// This must match BoundaryIndex(nextIndex, K) used by admission when building preloads.
		boundary := genEndIndex(g-1, c.GenerationThreshold)
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

	// Register callback to observe cache sizes
	registration, err := m.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(sizeGauge, int64(c.Volumes.Size()),
				metric.WithAttributes(attribute.String("type", "volumes")))
			o.ObserveInt64(sizeGauge, int64(c.AccountMetadata.Size()),
				metric.WithAttributes(attribute.String("type", "account_metadata")))
			o.ObserveInt64(sizeGauge, int64(c.IdempotencyKeys.Size()),
				metric.WithAttributes(attribute.String("type", "idempotency_keys")))
			o.ObserveInt64(sizeGauge, int64(c.References.Size()),
				metric.WithAttributes(attribute.String("type", "references")))
			o.ObserveInt64(sizeGauge, int64(c.Ledgers.Size()),
				metric.WithAttributes(attribute.String("type", "ledgers")))
			o.ObserveInt64(sizeGauge, int64(c.Boundaries.Size()),
				metric.WithAttributes(attribute.String("type", "boundaries")))
			o.ObserveInt64(sizeGauge, int64(c.SinkConfigs.Size()),
				metric.WithAttributes(attribute.String("type", "sink_configs")))
			o.ObserveInt64(sizeGauge, int64(c.NumscriptVersions.Size()),
				metric.WithAttributes(attribute.String("type", "numscript_versions")))
			o.ObserveInt64(sizeGauge, int64(c.NumscriptEntries.Size()),
				metric.WithAttributes(attribute.String("type", "numscript_entries")))

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
		BaseIndex:           newDualGen[uint64](0, 0),
		GenerationThreshold: generationThreshold,
	}
	ret.Volumes = newAttributeCache[*raftcmdpb.VolumePair](ret, "volumes")
	ret.AccountMetadata = newAttributeCache[*commonpb.MetadataValue](ret, "account_metadata")
	ret.IdempotencyKeys = newAttributeCache[*commonpb.IdempotencyKeyValue](ret, "idempotency_keys")
	ret.References = newAttributeCache[*commonpb.TransactionReferenceValue](ret, "references")
	ret.Ledgers = newAttributeCache[*commonpb.LedgerInfo](ret, "ledgers")
	ret.Boundaries = newAttributeCache[*raftcmdpb.LedgerBoundaries](ret, "boundaries")
	ret.SinkConfigs = newAttributeCache[*commonpb.SinkConfig](ret, "sink_configs")
	ret.NumscriptVersions = newAttributeCache[string](ret, "numscript_versions")
	ret.NumscriptEntries = newAttributeCache[bool](ret, "numscript_entries")

	err := ret.initMetrics(m)
	if err != nil {
		return nil, fmt.Errorf("initializing cache metrics: %w", err)
	}

	return ret, nil
}
