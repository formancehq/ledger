package admission

import (
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// numscriptEntryKey identifies a specific numscript version.
type numscriptEntryKey struct {
	Ledger  string
	Name    string
	Version string
}

// numscriptNameKey identifies a numscript by ledger and name (without version).
type numscriptNameKey struct {
	Ledger string
	Name   string
}

// overlay is a generic write-through overlay for intra-bulk data resolution.
// It tracks puts and deletes within a bulk so that later requests can see
// data written by earlier requests in the same bulk, before Pebble commit.
type overlay[K comparable, V any] struct {
	entries map[K]V
	deleted map[K]bool
}

func newOverlay[K comparable, V any]() *overlay[K, V] {
	return &overlay[K, V]{
		entries: make(map[K]V),
		deleted: make(map[K]bool),
	}
}

// Put stores a value in the overlay and clears any prior delete marker for this key.
func (o *overlay[K, V]) Put(key K, value V) {
	delete(o.deleted, key)
	o.entries[key] = value
}

// Delete marks a key as deleted and removes it from the entries.
// Callers should check IsDeleted before falling back to the external store.
func (o *overlay[K, V]) Delete(key K) {
	o.deleted[key] = true
	delete(o.entries, key)
}

// Get returns the value and true if the key exists in the overlay.
func (o *overlay[K, V]) Get(key K) (V, bool) {
	v, ok := o.entries[key]

	return v, ok
}

// IsDeleted returns true if the key was explicitly deleted in this bulk.
func (o *overlay[K, V]) IsDeleted(key K) bool {
	return o.deleted[key]
}

// Range calls fn for every live entry in the overlay.
// Iteration order is non-deterministic.
func (o *overlay[K, V]) Range(fn func(K, V) bool) {
	for k, v := range o.entries {
		if !fn(k, v) {
			return
		}
	}
}

// bulkOverlay groups all typed overlays for a single bulk request.
// Add new fields here when future data types need intra-bulk resolution.
type bulkOverlay struct {
	numscriptEntries *overlay[numscriptEntryKey, string]
	numscriptLatest  *overlay[numscriptNameKey, string]
	sinks            *overlay[string, *commonpb.SinkConfig]
}

func newBulkOverlay() *bulkOverlay {
	return &bulkOverlay{
		numscriptEntries: newOverlay[numscriptEntryKey, string](),
		numscriptLatest:  newOverlay[numscriptNameKey, string](),
		sinks:            newOverlay[string, *commonpb.SinkConfig](),
	}
}

// recordNumscriptSave records an immutable save in the overlay and advances the
// per-name latest to the greatest semver seen so far in this bulk, mirroring the
// FSM's max-pointer maintenance.
func (o *bulkOverlay) recordNumscriptSave(ledger, name, version, content string) {
	o.numscriptEntries.Put(numscriptEntryKey{Ledger: ledger, Name: name, Version: version}, content)

	nameKey := numscriptNameKey{Ledger: ledger, Name: name}
	if cur, ok := o.numscriptLatest.Get(nameKey); ok && !greaterSemver(version, cur) {
		return
	}

	o.numscriptLatest.Put(nameKey, version)
}

// greaterSemver reports whether a is a strictly greater full semver than b.
// A non-semver b (or empty) is treated as smaller so a valid save always wins.
func greaterSemver(a, b string) bool {
	av, aerr := semver.Parse(a)
	bv, berr := semver.Parse(b)
	if aerr != nil {
		return false
	}

	if berr != nil {
		return true
	}

	return av.Compare(bv) > 0
}
