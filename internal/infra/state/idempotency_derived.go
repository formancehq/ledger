package state

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// DerivedIdempotencyStore is the per-proposal overlay for idempotency keys.
// Writes go to pending and are flushed to the parent store + Pebble on Merge.
type DerivedIdempotencyStore struct {
	parent  *IdempotencyStore
	pending map[string]*commonpb.IdempotencyKeyValue
}

// NewDerivedIdempotencyStore creates a DerivedIdempotencyStore from a parent store.
func NewDerivedIdempotencyStore(parent *IdempotencyStore) *DerivedIdempotencyStore {
	return &DerivedIdempotencyStore{
		parent:  parent,
		pending: make(map[string]*commonpb.IdempotencyKeyValue),
	}
}

// Get checks pending first, then the parent in-memory map.
func (d *DerivedIdempotencyStore) Get(key string) (*commonpb.IdempotencyKeyValue, error) {
	if v, ok := d.pending[key]; ok {
		return v, nil
	}

	v, ok := d.parent.Get(key)
	if ok {
		return v, nil
	}

	return nil, domain.ErrNotFound
}

// Put writes a value to the pending overlay.
func (d *DerivedIdempotencyStore) Put(key string, value *commonpb.IdempotencyKeyValue) {
	d.pending[key] = value
}

// Merge flushes pending entries to Pebble (via SaveIdempotencyKey) and
// updates the parent in-memory map.
func (d *DerivedIdempotencyStore) Merge(batch *dal.Batch) error {
	for key, value := range d.pending {
		if err := SaveIdempotencyKey(batch, key, value); err != nil {
			return err
		}

		d.parent.Put(key, value)
	}

	return nil
}
