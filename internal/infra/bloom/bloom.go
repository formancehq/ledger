package bloom

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"math/bits"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
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
// the Pebble batch. Key format: [KeyPrefixBloom][attrCode][blockIndex BE 8].
func (f *Filter) PersistDirtyBlocks(batch *dal.Batch) error {
	for blockIdx, blk := range f.dirtyBlocks() {
		key := make([]byte, 1+1+8)
		key[0] = dal.KeyPrefixBloom
		key[1] = f.attrCode
		binary.BigEndian.PutUint64(key[2:], blockIdx)

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

// RestoreFromStore loads persisted bloom blocks from Pebble, replacing the
// in-memory filter contents. Blocks not found in Pebble remain zeroed.
func (f *Filter) RestoreFromStore(store dal.PebbleReader) error {
	lower := []byte{dal.KeyPrefixBloom, f.attrCode}
	upper := []byte{dal.KeyPrefixBloom, f.attrCode + 1}

	it, err := store.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("creating bloom restore iterator: %w", err)
	}

	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		// Key format: [0xE7][attrCode][blockIndex BE 8]
		if len(key) < 1+1+8 {
			continue
		}

		blockIdx := binary.BigEndian.Uint64(key[2:])
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

		f.filter.SetBlock(blockIdx, unmarshalBlock(val))
	}

	if err := it.Error(); err != nil {
		return fmt.Errorf("bloom restore iter error: %w", err)
	}

	return nil
}

// FilterSet holds per-attribute-type bloom filters.
// The ready flag indicates whether the bloom filters have been fully populated
// (from a Pebble restore + cache replay). While not ready, MayContain
// always returns true (= "maybe present") to avoid false negatives.
type FilterSet struct {
	ready atomic.Bool

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

	meter metric.Meter

	readyGauge metric.Int64Gauge
}

// FilterForAttrType returns the bloom filter for a given attribute type prefix byte.
func (fs *FilterSet) FilterForAttrType(attrType byte) *Filter {
	switch attrType {
	case dal.AttributeCodeVolume:
		return fs.Volume
	case dal.AttributeCodeMetadata:
		return fs.Metadata
	case dal.AttributeCodeReference:
		return fs.Reference
	case dal.AttributeCodeLedger:
		return fs.Ledger
	case dal.AttributeCodeBoundary:
		return fs.Boundary
	case dal.AttributeCodeTransaction:
		return fs.Transaction
	case dal.AttributeCodeSinkConfig:
		return fs.SinkConfig
	case dal.AttributeCodeNumscriptVersion:
		return fs.NumscriptVersion
	case dal.AttributeCodeNumscriptContent:
		return fs.NumscriptContent
	case dal.AttributeCodeLedgerMetadata:
		return fs.LedgerMetadata
	default:
		return nil
	}
}

// IsReady returns true if the bloom filters are fully populated.
func (fs *FilterSet) IsReady() bool {
	return fs.ready.Load()
}

// SetReady marks the bloom filters as fully populated.
func (fs *FilterSet) SetReady(v bool) {
	fs.ready.Store(v)

	val := int64(0)
	if v {
		val = 1
	}

	fs.readyGauge.Record(context.Background(), val)
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
	addKeys := func(f *Filter, ids []attributes.U128) {
		if f == nil {
			return
		}

		f.addBatch(ids)
	}

	addKeys(fs.Volume, updates.Volumes)
	addKeys(fs.Metadata, updates.Metadata)
	addKeys(fs.Reference, updates.References)
	addKeys(fs.Ledger, updates.Ledgers)
	addKeys(fs.Boundary, updates.Boundaries)
	addKeys(fs.Transaction, updates.Transactions)
	addKeys(fs.SinkConfig, updates.SinkConfigs)
	addKeys(fs.NumscriptVersion, updates.NumscriptVersions)
	addKeys(fs.NumscriptContent, updates.NumscriptContents)
	addKeys(fs.LedgerMetadata, updates.LedgerMetadata)
}

// PersistDirtyBlocks writes all dirty blocks from all filters to the Pebble batch.
// Called during cache rotation to flush bloom state atomically with the rotation.
func (fs *FilterSet) PersistDirtyBlocks(batch *dal.Batch) error {
	for _, f := range fs.allFilters() {
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
func (fs *FilterSet) RestoreFromStore(store dal.PebbleReader) error {
	for _, f := range fs.allFilters() {
		if f == nil {
			continue
		}

		if err := f.RestoreFromStore(store); err != nil {
			return err
		}
	}

	return nil
}

// PopulateFromStore scans the Pebble attribute range and inserts all existing
// canonical keys into the bloom filters. Used on first boot when no persisted
// bloom blocks exist yet.
func (fs *FilterSet) PopulateFromStore(store dal.PebbleReader) error {
	it, err := store.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.KeyPrefixAttributes},
		UpperBound: []byte{dal.KeyPrefixAttributes + 1},
	})
	if err != nil {
		return fmt.Errorf("creating attribute iterator: %w", err)
	}

	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
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
		id := attributes.HashU128(attributes.DefaultSeeds, canonicalKey)
		f.Add(id)
	}

	if err := it.Error(); err != nil {
		return fmt.Errorf("iterating attributes: %w", err)
	}

	fs.SetReady(true)

	return nil
}

// Rebuild recreates all bloom filters with new dimensions from the given
// ClusterConfig. Existing filter data is discarded. The caller must purge
// persisted blocks (0xE7) and trigger a full attribute scan to repopulate.
func (fs *FilterSet) Rebuild(cfg *commonpb.ClusterConfig) {
	// Mark not-ready BEFORE replacing filters to avoid a window where
	// concurrent readers see IsReady=true but get empty filters (false negatives).
	fs.SetReady(false)

	for _, bt := range bloomTypes(cfg) {
		bt.rebuild(fs)
	}
}

func (fs *FilterSet) allFilters() []*Filter {
	return []*Filter{
		fs.Volume, fs.Metadata, fs.Reference,
		fs.Ledger, fs.Boundary, fs.Transaction, fs.SinkConfig,
		fs.NumscriptVersion, fs.NumscriptContent, fs.LedgerMetadata,
	}
}

// bloomType maps a proto bloom config field to its Filter field and metadata.
type bloomType struct {
	cfg      *commonpb.BloomTypeConfig
	attrCode byte
	name     string
	field    func(fs *FilterSet) **Filter
}

func (bt bloomType) rebuild(fs *FilterSet) {
	dst := bt.field(fs)
	if bt.cfg.GetExpectedKeys() == 0 {
		*dst = nil

		return
	}

	*dst = newFilter(uint(bt.cfg.GetExpectedKeys()), bt.cfg.GetFpRate(), bt.attrCode, fs.meter, bt.name)
}

// bloomTypes returns the ordered list of bloom type descriptors from a ClusterConfig.
func bloomTypes(cfg *commonpb.ClusterConfig) []bloomType {
	return []bloomType{
		{cfg.GetBloomVolumes(), dal.AttributeCodeVolume, "volumes", func(fs *FilterSet) **Filter { return &fs.Volume }},
		{cfg.GetBloomMetadata(), dal.AttributeCodeMetadata, "metadata", func(fs *FilterSet) **Filter { return &fs.Metadata }},
		{cfg.GetBloomReferences(), dal.AttributeCodeReference, "references", func(fs *FilterSet) **Filter { return &fs.Reference }},
		{cfg.GetBloomLedgers(), dal.AttributeCodeLedger, "ledgers", func(fs *FilterSet) **Filter { return &fs.Ledger }},
		{cfg.GetBloomBoundaries(), dal.AttributeCodeBoundary, "boundaries", func(fs *FilterSet) **Filter { return &fs.Boundary }},
		{cfg.GetBloomTransactions(), dal.AttributeCodeTransaction, "transactions", func(fs *FilterSet) **Filter { return &fs.Transaction }},
		{cfg.GetBloomSinkConfigs(), dal.AttributeCodeSinkConfig, "sink_configs", func(fs *FilterSet) **Filter { return &fs.SinkConfig }},
		{cfg.GetBloomNumscriptVersions(), dal.AttributeCodeNumscriptVersion, "numscript_versions", func(fs *FilterSet) **Filter { return &fs.NumscriptVersion }},
		{cfg.GetBloomNumscriptContents(), dal.AttributeCodeNumscriptContent, "numscript_contents", func(fs *FilterSet) **Filter { return &fs.NumscriptContent }},
		{cfg.GetBloomLedgerMetadata(), dal.AttributeCodeLedgerMetadata, "ledger_metadata", func(fs *FilterSet) **Filter { return &fs.LedgerMetadata }},
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

	for _, bt := range bloomTypes(cfg) {
		bt.rebuild(fs)
	}

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
