package bloom

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greatroar/blobloom"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// FilterConfig holds the configuration for a single bloom filter.
type FilterConfig struct {
	ExpectedKeys uint
	FPRate       float64
}

// FilterSetConfig holds per-attribute-type bloom filter configurations.
type FilterSetConfig struct {
	Volume      FilterConfig
	Metadata    FilterConfig
	Idempotency FilterConfig
	Reference   FilterConfig
	Ledger      FilterConfig
	Boundary    FilterConfig
	Transaction FilterConfig
}

// Enabled returns true if at least one attribute type has a non-zero expected key count.
func (c FilterSetConfig) Enabled() bool {
	for _, fc := range c.AsList() {
		if fc.ExpectedKeys > 0 {
			return true
		}
	}

	return false
}

// AsList returns all per-type configs in a fixed order (volume, metadata, idempotency,
// reference, ledger, boundary, transaction) for deterministic serialization.
func (c FilterSetConfig) AsList() [7]FilterConfig {
	return [7]FilterConfig{
		c.Volume, c.Metadata, c.Idempotency, c.Reference,
		c.Ledger, c.Boundary, c.Transaction,
	}
}

// DefaultFilterSetConfig returns defaults with all bloom filters disabled.
// Operators can enable individual filters via CLI flags (e.g.
// --bloom-volumes-expected-keys=100000000 --bloom-volumes-fp-rate=0.01).
func DefaultFilterSetConfig() FilterSetConfig {
	return FilterSetConfig{}
}

// Filter wraps a blocked bloom filter with lock-free atomic operations.
// SyncFilter supports concurrent reads (admission goroutines) and single-writer
// updates (FSM goroutine) without any mutex.
type Filter struct {
	filter *blobloom.SyncFilter

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
// Lock-free via SyncFilter atomics.
func (f *Filter) Add(id attributes.U128) {
	f.filter.Add(id.Hi())
	f.adds.Add(context.Background(), 1)
}

// WriteTo serializes the bloom filter to the writer.
func (f *Filter) WriteTo(w io.Writer) (int64, error) {
	return blobloom.DumpSync(w, f.filter, "")
}

// LoadFrom deserializes a bloom filter from the reader, replacing the current filter.
func (f *Filter) LoadFrom(r io.Reader) error {
	loader, err := blobloom.NewLoader(r)
	if err != nil {
		return fmt.Errorf("creating bloom loader: %w", err)
	}

	loaded, err := loader.LoadSync(f.filter)
	if err != nil {
		return fmt.Errorf("loading bloom filter: %w", err)
	}

	f.filter = loaded

	return nil
}

// FilterSet holds per-attribute-type bloom filters.
// The ready flag indicates whether the bloom filters have been fully populated
// (from a checkpoint restore or background scan). While not ready, MayContain
// always returns true (= "maybe present") to avoid false negatives.
type FilterSet struct {
	ready atomic.Bool

	Volume      *Filter
	Metadata    *Filter
	Idempotency *Filter
	Reference   *Filter
	Ledger      *Filter
	Boundary    *Filter
	Transaction *Filter

	config FilterSetConfig

	readyGauge metric.Int64Gauge
}

// FilterForAttrType returns the bloom filter for a given attribute type prefix byte.
func (fs *FilterSet) FilterForAttrType(attrType byte) *Filter {
	switch attrType {
	case dal.AttributePrefixVolume:
		return fs.Volume
	case dal.AttributePrefixMetadata:
		return fs.Metadata
	case dal.AttributePrefixIdempotency:
		return fs.Idempotency
	case dal.AttributePrefixReference:
		return fs.Reference
	case dal.AttributePrefixLedger:
		return fs.Ledger
	case dal.AttributePrefixBoundary:
		return fs.Boundary
	case dal.AttributePrefixTransaction:
		return fs.Transaction
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

// BloomUpdates holds canonical keys collected during Merge for bloom filter updates.
type BloomUpdates struct {
	Volumes      [][]byte
	Metadata     [][]byte
	Idempotency  [][]byte
	References   [][]byte
	Ledgers      [][]byte
	Boundaries   [][]byte
	Transactions [][]byte
}

// Reset clears all slices while preserving their backing arrays.
func (u *BloomUpdates) Reset() {
	u.Volumes = u.Volumes[:0]
	u.Metadata = u.Metadata[:0]
	u.Idempotency = u.Idempotency[:0]
	u.References = u.References[:0]
	u.Ledgers = u.Ledgers[:0]
	u.Boundaries = u.Boundaries[:0]
	u.Transactions = u.Transactions[:0]
}

// AddCanonicalKeys hashes canonical keys and inserts them into the corresponding bloom filters.
// Called from the FSM goroutine after buffer.Merge() and before batch.Commit().
func (fs *FilterSet) AddCanonicalKeys(updates *BloomUpdates) {
	addKeys := func(f *Filter, keys [][]byte) {
		if f == nil {
			return
		}

		for _, key := range keys {
			id := attributes.HashU128(attributes.DefaultSeeds, key)
			f.filter.Add(id.Hi())
		}

		f.adds.Add(context.Background(), int64(len(keys)))
	}

	addKeys(fs.Volume, updates.Volumes)
	addKeys(fs.Metadata, updates.Metadata)
	addKeys(fs.Idempotency, updates.Idempotency)
	addKeys(fs.Reference, updates.References)
	addKeys(fs.Ledger, updates.Ledgers)
	addKeys(fs.Boundary, updates.Boundaries)
	addKeys(fs.Transaction, updates.Transactions)
}

// Cache snapshot sub-prefix for bloom filter data within the 0xFF zone.
// Key layout:
// PopulateFromStore scans the Pebble attribute range and inserts all existing
// canonical keys into the bloom filters. Used on first boot when no bloom
// snapshot exists yet.
func (fs *FilterSet) PopulateFromStore(store dal.PebbleReader) error {
	iter, err := store.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.KeyPrefixAttributes},
		UpperBound: []byte{dal.KeyPrefixAttributes + 1},
	})
	if err != nil {
		return fmt.Errorf("creating attribute iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		pebbleKey := iter.Key()

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
		f.filter.Add(id.Hi())
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating attributes: %w", err)
	}

	fs.SetReady(true)

	return nil
}

func newFilter(expectedKeys uint, fpRate float64, meter metric.Meter, typeName string) *Filter {
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

	return &Filter{
		filter: blobloom.NewSyncOptimized(blobloom.Config{
			Capacity: uint64(expectedKeys),
			FPRate:   fpRate,
		}),
		lookups:        withAttr(lookups, typeAttr),
		negatives:      withAttr(negatives, typeAttr),
		falsePositives: withAttr(falsePositives, typeAttr),
		adds:           withAttr(adds, typeAttr),
	}
}

// newFilterOrNil creates a bloom filter for the given config, or returns nil if disabled.
func newFilterOrNil(cfg FilterConfig, meter metric.Meter, typeName string) *Filter {
	if cfg.ExpectedKeys == 0 {
		return nil
	}

	return newFilter(cfg.ExpectedKeys, cfg.FPRate, meter, typeName)
}

// NewFilterSet creates a new FilterSet with per-type bloom filters.
// Returns nil if no attribute type is enabled (all ExpectedKeys == 0).
func NewFilterSet(cfg FilterSetConfig, meter metric.Meter) *FilterSet {
	if !cfg.Enabled() {
		return nil
	}

	if meter == nil {
		meter = noop.Meter{}
	}

	readyGauge, _ := meter.Int64Gauge(
		"bloom.ready",
		metric.WithDescription("Bloom filter readiness (1 = ready, 0 = populating)"),
	)

	return &FilterSet{
		Volume:      newFilterOrNil(cfg.Volume, meter, "volumes"),
		Metadata:    newFilterOrNil(cfg.Metadata, meter, "metadata"),
		Idempotency: newFilterOrNil(cfg.Idempotency, meter, "idempotency"),
		Reference:   newFilterOrNil(cfg.Reference, meter, "references"),
		Ledger:      newFilterOrNil(cfg.Ledger, meter, "ledgers"),
		Boundary:    newFilterOrNil(cfg.Boundary, meter, "boundaries"),
		Transaction: newFilterOrNil(cfg.Transaction, meter, "transactions"),
		config:      cfg,
		readyGauge:  readyGauge,
	}
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
