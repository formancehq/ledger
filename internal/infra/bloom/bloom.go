package bloom

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// BloomConfigEnabled returns true if at least one bloom filter type has a
// non-zero expected key count in the given ClusterConfig.
func BloomConfigEnabled(cfg *commonpb.ClusterConfig) bool {
	for _, tc := range bloomTypes(cfg) {
		if tc.cfg.GetExpectedKeys() > 0 {
			return true
		}
	}

	return false
}

// BloomConfigEqual returns true if two ClusterConfigs have identical bloom
// filter settings.
func BloomConfigEqual(a, b *commonpb.ClusterConfig) bool {
	for i, at := range bloomTypes(a) {
		bt := bloomTypes(b)[i]
		if at.cfg.GetExpectedKeys() != bt.cfg.GetExpectedKeys() || at.cfg.GetFpRate() != bt.cfg.GetFpRate() {
			return false
		}
	}

	return true
}

// Filter wraps a blocked bloom filter with lock-free atomic operations and
// dirty-block tracking for incremental Pebble persistence.
type Filter struct {
	filter *blockedFilter

	// attrCode is the AttributeCode* byte used as Pebble key component.
	attrCode byte

	// dirty is a bitset with one bit per block, tracking which blocks have
	// been modified since the last flush. Accessed with atomic operations
	// for concurrent safety (FSM goroutine + background populate).
	dirty []uint64

	lookups        metric.Int64Counter
	negatives      metric.Int64Counter
	falsePositives metric.Int64Counter
	adds           metric.Int64Counter
}

// MayContain returns true if the key might be in the set, false if it is
// definitely not. Thread-safe for concurrent readers (lock-free atomics).
func (f *Filter) MayContain(id attributes.U128) bool {
	result := f.filter.Has(id.Hi())

	f.lookups.Add(context.Background(), 1)

	if !result {
		f.negatives.Add(context.Background(), 1)
	}

	return result
}

// RecordFalsePositive increments the false positive counter. Called by the
// preloader when MayContain returned true but the Pebble Get found no value.
func (f *Filter) RecordFalsePositive() {
	f.falsePositives.Add(context.Background(), 1)
}

// Add inserts a key into the bloom filter. Called from the FSM goroutine only.
// Lock-free via atomic operations. The touched block is marked dirty.
func (f *Filter) Add(id attributes.U128) {
	f.add(id)
	f.adds.Add(context.Background(), 1)
}

// addBatch inserts multiple keys into the bloom filter and increments the OTel
// counter once for the entire batch. This avoids per-key OTel overhead on the
// FSM hot path.
func (f *Filter) addBatch(ids []attributes.U128) {
	for _, id := range ids {
		f.add(id)
	}

	if len(ids) > 0 {
		f.adds.Add(context.Background(), int64(len(ids)))
	}
}

func (f *Filter) add(id attributes.U128) {
	idx := f.filter.Add(id.Hi())
	atomic.OrUint64(&f.dirty[idx/64], 1<<(uint64(idx)%64))
}

// PersistDirtyBlocks writes all blocks modified since the last flush to
// the Pebble batch. Key format: [ZoneGlobal][SubGlobBloom][attrCode][blockIndex BE 8].
func (f *Filter) PersistDirtyBlocks(batch *dal.Batch) error {
	for blockIdx, blk := range f.dirtyBlocks() {
		key := make([]byte, 2+1+8)
		key[0] = dal.ZoneGlobal
		key[1] = dal.SubGlobBloom
		key[2] = f.attrCode
		binary.BigEndian.PutUint64(key[3:], blockIdx)

		if err := batch.Set(key, marshalBlock(&blk), pebble.NoSync); err != nil {
			return fmt.Errorf("persisting bloom block %d: %w", blockIdx, err)
		}
	}

	for i := range f.dirty {
		atomic.StoreUint64(&f.dirty[i], 0)
	}

	return nil
}

// dirtyBlocks returns an iterator over (blockIndex, block) pairs for all
// blocks that have been modified since the last flush.
func (f *Filter) dirtyBlocks() iter.Seq2[uint64, block] {
	return func(yield func(uint64, block) bool) {
		for wi := range f.dirty {
			word := atomic.LoadUint64(&f.dirty[wi])
			if word == 0 {
				continue
			}

			for word != 0 {
				bit := word & (^word + 1) // isolate lowest set bit
				bitIdx := bits.TrailingZeros64(word)
				blockIdx := uint64(wi)*64 + uint64(bitIdx)

				if blockIdx >= f.filter.BlockCount() {
					return
				}

				if !yield(blockIdx, f.filter.GetBlock(blockIdx)) {
					return
				}

				word ^= bit
			}
		}
	}
}

// RestoreFromStore loads persisted bloom blocks from Pebble, merging them
// into the in-memory filter via OR. This preserves bits set by concurrent
// Add() calls from the FSM goroutine during the async restore window.
func (f *Filter) RestoreFromStore(ctx context.Context, store dal.PebbleReader) error {
	lower := []byte{dal.ZoneGlobal, dal.SubGlobBloom, f.attrCode}
	upper := []byte{dal.ZoneGlobal, dal.SubGlobBloom, f.attrCode + 1}

	it, err := store.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("creating bloom restore iterator: %w", err)
	}

	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		key := it.Key()
		// Key format: [ZoneGlobal][SubGlobBloom][attrCode][blockIndex BE 8]
		if len(key) < 2+1+8 {
			continue
		}

		blockIdx := binary.BigEndian.Uint64(key[3:])
		if blockIdx >= f.filter.BlockCount() {
			continue
		}

		val, err := it.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading bloom block %d: %w", blockIdx, err)
		}

		if len(val) < blockBytes {
			continue
		}

		f.filter.OrBlock(blockIdx, unmarshalBlock(val))
	}

	if err := it.Error(); err != nil {
		return fmt.Errorf("bloom restore iter error: %w", err)
	}

	return nil
}

// filterSnapshot holds per-attribute-type bloom filter pointers plus the
// readiness flag that goes with them. Accessed via atomic.Pointer in
// FilterSet so that a reader can observe filters and readiness in a single
// atomic load — never pair a "ready" flag from one revision with a
// freshly-swapped empty snapshot from another (#317).
//
// Fields are treated as immutable once published: Rebuild and
// SetReadyIfEpoch allocate a new filterSnapshot rather than mutating the
// existing one. Filter pointers themselves remain mutable through their
// own thread-safe API (Filter.Add / MayContain) so in-flight FSM writes
// during the populator window are preserved across the not-ready → ready
// transition.
type filterSnapshot struct {
	Volume           *Filter
	Metadata         *Filter
	Reference        *Filter
	Ledger           *Filter
	Boundary         *Filter
	Transaction      *Filter
	SinkConfig       *Filter
	NumscriptVersion *Filter
	NumscriptContent *Filter
	LedgerMetadata   *Filter
	// ready marks the snapshot as fully populated. Bound to the snap so a
	// reader that captures the snap pointer sees consistent (filters, ready).
	ready bool
}

func (s *filterSnapshot) filterForAttrType(attrType byte) *Filter {
	switch attrType {
	case dal.SubAttrVolume:
		return s.Volume
	case dal.SubAttrMetadata:
		return s.Metadata
	case dal.SubAttrReference:
		return s.Reference
	case dal.SubAttrLedger:
		return s.Ledger
	case dal.SubAttrBoundary:
		return s.Boundary
	case dal.SubAttrTransaction:
		return s.Transaction
	case dal.SubAttrSinkConfig:
		return s.SinkConfig
	case dal.SubAttrNumscriptVersion:
		return s.NumscriptVersion
	case dal.SubAttrNumscriptContent:
		return s.NumscriptContent
	case dal.SubAttrLedgerMetadata:
		return s.LedgerMetadata
	default:
		return nil
	}
}

func (s *filterSnapshot) allFilters() []*Filter {
	return []*Filter{
		s.Volume, s.Metadata, s.Reference,
		s.Ledger, s.Boundary, s.Transaction, s.SinkConfig,
		s.NumscriptVersion, s.NumscriptContent, s.LedgerMetadata,
	}
}

// FilterSet holds per-attribute-type bloom filters. Readiness is carried
// inside the snapshot pointer so a single atomic.Pointer load resolves both
// (filters, ready) consistently — see filterSnapshot for the rationale.
//
// Concurrent readers (preloader) never race with Rebuild (Raft goroutine)
// because the snapshot pointer is updated atomically and never mutated.
type FilterSet struct {
	filters atomic.Pointer[filterSnapshot]

	// readyMu serializes Rebuild (which publishes a not-ready snapshot and
	// bumps epoch) with background goroutines that want to flip the current
	// snapshot to ready. Without this, the epoch check and the snap swap
	// in the background goroutine are not atomic and Rebuild can slip in
	// between (TOCTOU). Readers remain lock-free — only writers take it.
	readyMu sync.Mutex
	epoch   uint64

	meter metric.Meter

	readyGauge metric.Int64Gauge
}

// FilterForAttrType returns the bloom filter for a given attribute type prefix byte.
// Safe for concurrent use — reads an atomic snapshot of filter pointers.
func (fs *FilterSet) FilterForAttrType(attrType byte) *Filter {
	snap := fs.filters.Load()
	if snap == nil {
		return nil
	}

	return snap.filterForAttrType(attrType)
}

// Snapshot returns an opaque handle to the current filter snapshot.
// Use with FilterFromSnapshot to get individual filters. This ensures
// all attribute types are resolved from the same snapshot, avoiding
// inconsistencies when Rebuild swaps the pointer between lookups.
func (fs *FilterSet) Snapshot() FilterSnapshot {
	return FilterSnapshot{snap: fs.filters.Load()}
}

// FilterSnapshot is an opaque handle to a consistent set of bloom filters
// AND the readiness state that goes with them. Capture once via
// FilterSet.Snapshot, branch on Ready, then read filters — no second
// atomic load and no race against Rebuild.
type FilterSnapshot struct {
	snap *filterSnapshot
}

// Ready reports whether this snapshot is fully populated. False on a nil
// snap (FilterSet was nil) and on snapshots published by Rebuild before
// the populator has flipped them to ready.
func (s FilterSnapshot) Ready() bool {
	return s.snap != nil && s.snap.ready
}

// FilterForAttrType returns the bloom filter for the given attribute type
// from this snapshot. Returns nil on a nil snap.
func (s FilterSnapshot) FilterForAttrType(attrType byte) *Filter {
	if s.snap == nil {
		return nil
	}

	return s.snap.filterForAttrType(attrType)
}

// IsReady reports whether the current snapshot is fully populated. This
// is a single-load convenience; callers that subsequently read filters
// MUST use Snapshot() and branch on Snapshot.Ready() instead — otherwise
// they race against Rebuild between the IsReady call and the filter
// lookup (#317).
func (fs *FilterSet) IsReady() bool {
	snap := fs.filters.Load()

	return snap != nil && snap.ready
}

// SetReady publishes a new snapshot with the requested readiness, reusing
// the current filter pointers. Concurrent SetReadyIfEpoch / Rebuild calls
// are serialised through readyMu so the snap copy can't race them.
func (fs *FilterSet) SetReady(v bool) {
	fs.readyMu.Lock()
	fs.setReadyLocked(v)
	fs.readyMu.Unlock()
}

// setReadyLocked publishes a new snapshot with the requested readiness.
// Caller must hold readyMu. Filter pointers are shared with the previous
// snapshot — in-flight FSM writes during a populator window land in the
// same Filter objects and become visible the moment the ready snap is
// published.
func (fs *FilterSet) setReadyLocked(v bool) {
	old := fs.filters.Load()

	var next *filterSnapshot
	switch {
	case old == nil:
		next = &filterSnapshot{ready: v}
	case old.ready == v:
		// Already in the target state — no need to allocate a new snap.
		return
	default:
		clone := *old
		clone.ready = v
		next = &clone
	}

	fs.filters.Store(next)

	val := int64(0)
	if v {
		val = 1
	}

	fs.readyGauge.Record(context.Background(), val)
}

// Epoch returns the current rebuild epoch under readyMu.
func (fs *FilterSet) Epoch() uint64 {
	fs.readyMu.Lock()
	defer fs.readyMu.Unlock()

	return fs.epoch
}

// SetReadyIfEpoch publishes a ready snapshot only if the epoch has not
// changed since the caller captured it. Returns true if the snapshot was
// promoted. This prevents a stale background goroutine from marking
// empty post-Rebuild filters as ready (TOCTOU).
func (fs *FilterSet) SetReadyIfEpoch(expected uint64) bool {
	fs.readyMu.Lock()
	defer fs.readyMu.Unlock()

	if fs.epoch != expected {
		return false
	}

	fs.setReadyLocked(true)

	return true
}

// BloomUpdates holds pre-hashed U128 IDs collected during Merge for bloom filter updates.
// Keys are hashed at collection time (in mergeAndTrackBloom) to avoid redundant hashing
// in AddCanonicalKeys on the FSM hot path.
type BloomUpdates struct {
	Volumes           []attributes.U128
	Metadata          []attributes.U128
	References        []attributes.U128
	Ledgers           []attributes.U128
	Boundaries        []attributes.U128
	Transactions      []attributes.U128
	SinkConfigs       []attributes.U128
	NumscriptVersions []attributes.U128
	NumscriptContents []attributes.U128
	PreparedQueries   []attributes.U128
	LedgerMetadata    []attributes.U128
}

// Reset clears all slices while preserving their backing arrays.
func (u *BloomUpdates) Reset() {
	u.Volumes = u.Volumes[:0]
	u.Metadata = u.Metadata[:0]
	u.References = u.References[:0]
	u.Ledgers = u.Ledgers[:0]
	u.Boundaries = u.Boundaries[:0]
	u.Transactions = u.Transactions[:0]
	u.SinkConfigs = u.SinkConfigs[:0]
	u.NumscriptVersions = u.NumscriptVersions[:0]
	u.NumscriptContents = u.NumscriptContents[:0]
	u.PreparedQueries = u.PreparedQueries[:0]
	u.LedgerMetadata = u.LedgerMetadata[:0]
}

// AddCanonicalKeys inserts pre-hashed U128 IDs into the corresponding bloom filters.
// Called from the FSM goroutine after buffer.Merge() and before batch.Commit().
// OTel counters are incremented once per filter (not per key) to reduce hot-path overhead.
func (fs *FilterSet) AddCanonicalKeys(updates *BloomUpdates) {
	snap := fs.filters.Load()
	if snap == nil {
		return
	}

	addKeys := func(f *Filter, ids []attributes.U128) {
		if f == nil {
			return
		}

		f.addBatch(ids)
	}

	addKeys(snap.Volume, updates.Volumes)
	addKeys(snap.Metadata, updates.Metadata)
	addKeys(snap.Reference, updates.References)
	addKeys(snap.Ledger, updates.Ledgers)
	addKeys(snap.Boundary, updates.Boundaries)
	addKeys(snap.Transaction, updates.Transactions)
	addKeys(snap.SinkConfig, updates.SinkConfigs)
	addKeys(snap.NumscriptVersion, updates.NumscriptVersions)
	addKeys(snap.NumscriptContent, updates.NumscriptContents)
	addKeys(snap.LedgerMetadata, updates.LedgerMetadata)
}

// PersistDirtyBlocks writes all dirty blocks from all filters to the Pebble batch.
// Called during cache rotation to flush bloom state atomically with the rotation.
func (fs *FilterSet) PersistDirtyBlocks(batch *dal.Batch) error {
	snap := fs.filters.Load()
	if snap == nil {
		return nil
	}

	for _, f := range snap.allFilters() {
		if f == nil {
			continue
		}

		if err := f.PersistDirtyBlocks(batch); err != nil {
			return err
		}
	}

	return nil
}

// RestoreFromStore loads all persisted bloom blocks from Pebble.
func (fs *FilterSet) RestoreFromStore(ctx context.Context, store dal.PebbleReader) error {
	snap := fs.filters.Load()
	if snap == nil {
		return nil
	}

	for _, f := range snap.allFilters() {
		if f == nil {
			continue
		}

		if err := f.RestoreFromStore(ctx, store); err != nil {
			return err
		}
	}

	return nil
}

// PopulateFromStore scans the Pebble attribute range and inserts all existing
// canonical keys into the bloom filters. Used on first boot when no persisted
// bloom blocks exist yet.
func (fs *FilterSet) PopulateFromStore(ctx context.Context, store dal.PebbleReader) error {
	it, err := store.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneAttributes},
		UpperBound: []byte{dal.ZoneAttributes + 1},
	})
	if err != nil {
		return fmt.Errorf("creating attribute iterator: %w", err)
	}

	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		pebbleKey := it.Key()

		attrType, ok := attributes.AttrTypeFromKey(pebbleKey)
		if !ok {
			continue
		}

		f := fs.FilterForAttrType(attrType)
		if f == nil {
			continue
		}

		canonicalKey := attributes.CanonicalKeyFromPebbleKey(pebbleKey)
		id := attributes.HashU128(canonicalKey)
		f.Add(id)
	}

	if err := it.Error(); err != nil {
		return fmt.Errorf("iterating attributes: %w", err)
	}

	return nil
}

// Rebuild recreates all bloom filters with new dimensions from the given
// ClusterConfig. Existing filter data is discarded. The caller must purge
// persisted blocks (0xE7) and trigger a full attribute scan to repopulate.
//
// The new snapshot is published as not-ready, so any reader that observes
// it via Snapshot().Ready() sees false and falls back to the no-bloom
// path. Without that coupling, a reader could observe ready=true from an
// earlier snapshot and then load the new, still-empty one in a separate
// atomic read, treating present keys as absent (#317).
func (fs *FilterSet) Rebuild(cfg *commonpb.ClusterConfig) {
	snap := &filterSnapshot{}
	for _, bt := range bloomTypes(cfg) {
		bt.rebuild(snap, fs.meter)
	}

	fs.readyMu.Lock()
	// Bump epoch under the lock so a background SetReadyIfEpoch using the
	// previous epoch cannot promote this fresh, still-empty snapshot.
	fs.epoch++
	fs.filters.Store(snap)
	fs.readyMu.Unlock()

	fs.readyGauge.Record(context.Background(), 0)
}

// bloomType maps a proto bloom config field to its Filter field and metadata.
type bloomType struct {
	cfg      *commonpb.BloomTypeConfig
	attrCode byte
	name     string
	field    func(snap *filterSnapshot) **Filter
}

func (bt bloomType) rebuild(snap *filterSnapshot, meter metric.Meter) {
	dst := bt.field(snap)
	if bt.cfg.GetExpectedKeys() == 0 {
		*dst = nil

		return
	}

	*dst = newFilter(uint(bt.cfg.GetExpectedKeys()), bt.cfg.GetFpRate(), bt.attrCode, meter, bt.name)
}

// bloomTypes returns the ordered list of bloom type descriptors from a ClusterConfig.
func bloomTypes(cfg *commonpb.ClusterConfig) []bloomType {
	return []bloomType{
		{cfg.GetBloomVolumes(), dal.SubAttrVolume, "volumes", func(snap *filterSnapshot) **Filter { return &snap.Volume }},
		{cfg.GetBloomMetadata(), dal.SubAttrMetadata, "metadata", func(snap *filterSnapshot) **Filter { return &snap.Metadata }},
		{cfg.GetBloomReferences(), dal.SubAttrReference, "references", func(snap *filterSnapshot) **Filter { return &snap.Reference }},
		{cfg.GetBloomLedgers(), dal.SubAttrLedger, "ledgers", func(snap *filterSnapshot) **Filter { return &snap.Ledger }},
		{cfg.GetBloomBoundaries(), dal.SubAttrBoundary, "boundaries", func(snap *filterSnapshot) **Filter { return &snap.Boundary }},
		{cfg.GetBloomTransactions(), dal.SubAttrTransaction, "transactions", func(snap *filterSnapshot) **Filter { return &snap.Transaction }},
		{cfg.GetBloomSinkConfigs(), dal.SubAttrSinkConfig, "sink_configs", func(snap *filterSnapshot) **Filter { return &snap.SinkConfig }},
		{cfg.GetBloomNumscriptVersions(), dal.SubAttrNumscriptVersion, "numscript_versions", func(snap *filterSnapshot) **Filter { return &snap.NumscriptVersion }},
		{cfg.GetBloomNumscriptContents(), dal.SubAttrNumscriptContent, "numscript_contents", func(snap *filterSnapshot) **Filter { return &snap.NumscriptContent }},
		{cfg.GetBloomLedgerMetadata(), dal.SubAttrLedgerMetadata, "ledger_metadata", func(snap *filterSnapshot) **Filter { return &snap.LedgerMetadata }},
	}
}

func newFilter(expectedKeys uint, fpRate float64, attrCode byte, meter metric.Meter, typeName string) *Filter {
	typeAttr := attribute.String("type", typeName)

	lookups, _ := meter.Int64Counter(
		"bloom.lookups",
		metric.WithDescription("Total bloom filter checks"),
	)

	negatives, _ := meter.Int64Counter(
		"bloom.negatives",
		metric.WithDescription("Bloom filter checks that returned definitely-not-present (Pebble Gets avoided)"),
	)

	adds, _ := meter.Int64Counter(
		"bloom.adds",
		metric.WithDescription("Keys added to bloom filter"),
	)

	falsePositives, _ := meter.Int64Counter(
		"bloom.false_positives",
		metric.WithDescription("Bloom filter checks that returned maybe-present but Pebble Get found nothing"),
	)

	bf := newBlockedFilterOptimized(uint64(expectedKeys), fpRate)

	return &Filter{
		filter:         bf,
		attrCode:       attrCode,
		dirty:          make([]uint64, (bf.BlockCount()+63)/64),
		lookups:        withAttr(lookups, typeAttr),
		negatives:      withAttr(negatives, typeAttr),
		falsePositives: withAttr(falsePositives, typeAttr),
		adds:           withAttr(adds, typeAttr),
	}
}

// NewFilterSet creates a new FilterSet with per-type bloom filters from a
// ClusterConfig. Returns nil if no bloom filter type is enabled.
func NewFilterSet(cfg *commonpb.ClusterConfig, meter metric.Meter) *FilterSet {
	if !BloomConfigEnabled(cfg) {
		return nil
	}

	if meter == nil {
		meter = noop.Meter{}
	}

	readyGauge, _ := meter.Int64Gauge(
		"bloom.ready",
		metric.WithDescription("Bloom filter readiness (1 = ready, 0 = populating)"),
	)

	fs := &FilterSet{
		meter:      meter,
		readyGauge: readyGauge,
	}

	snap := &filterSnapshot{}
	for _, bt := range bloomTypes(cfg) {
		bt.rebuild(snap, meter)
	}

	fs.filters.Store(snap)

	return fs
}

// attrCounter wraps a counter to always include a fixed attribute.
type attrCounter struct {
	metric.Int64Counter

	attr attribute.KeyValue
}

func (c *attrCounter) Add(ctx context.Context, incr int64, _ ...metric.AddOption) {
	c.Int64Counter.Add(ctx, incr, metric.WithAttributes(c.attr))
}

func withAttr(c metric.Int64Counter, attr attribute.KeyValue) metric.Int64Counter {
	return &attrCounter{Int64Counter: c, attr: attr}
}
