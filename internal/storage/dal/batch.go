package dal

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"
)

// vtSizedMarshaler is implemented by vtprotobuf-generated messages.
type vtSizedMarshaler interface {
	SizeVT() int
	MarshalToVT([]byte) (int, error)
}

// deferredEntry is a point operation (Set or Delete) buffered in the arena.
// On Commit, entries are sorted by key so that Pebble's skiplist Inserter
// can use its splice forward-scan optimization (O(1) amortized per insert
// instead of O(log N) full traversal).
type deferredEntry struct {
	keyStart   uint32
	keyEnd     uint32
	valueStart uint32
	valueEnd   uint32
	isDelete   bool
}

// Batch provides atomic operations on the store using a pebble.Batch with NoSync.
//
// By default, point operations (Set, Delete) are written directly to the
// underlying pebble.Batch. When sorted mode is enabled via EnableSortedCommit,
// operations are instead buffered in a contiguous arena and sorted by key
// before being applied at Commit time. This exploits Pebble's skiplist Inserter
// splice optimization: sorted keys turn O(log N) traversals into O(1) amortized
// forward scans.
//
// Range operations (DeleteRange) are always applied immediately to the pebble.Batch.
//
// Cancel must be called if the batch is not committed to release resources.
type Batch struct {
	store          *Store
	batch          *pebble.Batch
	KeyBuilder     *KeyBuilder
	protoBuffer    []byte
	CacheBuffer    []byte // reusable buffer for 0xFF cache zone writes (tag+value)
	committed      bool
	sorted         bool
	marshalOptions proto.MarshalOptions

	// arena holds all deferred key and value bytes in a single contiguous buffer.
	// entries indexes into this arena via start/end offsets.
	// Only used when sorted=true.
	arena   []byte
	entries []deferredEntry
}

// MarshalProto marshals a proto message using vtprotobuf when available,
// falling back to standard MarshalAppend otherwise.
func (b *Batch) MarshalProto(msg proto.Message) ([]byte, error) {
	if m, ok := msg.(vtSizedMarshaler); ok {
		size := m.SizeVT()
		if cap(b.protoBuffer) >= size {
			b.protoBuffer = b.protoBuffer[:size]
		} else {
			b.protoBuffer = make([]byte, size)
		}

		n, err := m.MarshalToVT(b.protoBuffer)

		return b.protoBuffer[:n], err
	}

	return b.marshalOptions.MarshalAppend(b.protoBuffer, msg)
}

// NewBatch creates a new Batch for atomic operations.
func (s *Store) NewBatch() *Batch {
	return &Batch{
		store:       s,
		batch:       s.getDB().NewBatch(),
		KeyBuilder:  NewKeyBuilder(),
		protoBuffer: make([]byte, 0, 1024),
		CacheBuffer: make([]byte, 0, 128),
	}
}

// NewBatchFromDB creates a Batch backed by the given Pebble DB without a Store.
// Used by subsystems (e.g. readstore) that manage their own Pebble instance.
func NewBatchFromDB(db *pebble.DB) *Batch {
	return &Batch{
		batch:       db.NewBatch(),
		KeyBuilder:  NewKeyBuilder(),
		protoBuffer: make([]byte, 0, 1024),
	}
}

// EnableSortedCommit switches the batch to sorted mode: subsequent Set/Delete
// operations are buffered in an arena and sorted by key at Commit time.
// This should be called immediately after NewBatch, before any writes.
//
// Use this for large batches (100+ entries) where the Pebble skiplist insertion
// cost dominates. For small batches, the direct-write default is faster.
func (b *Batch) EnableSortedCommit() {
	b.sorted = true
}

// appendToArena copies data into the arena and returns the start and end offsets.
func (b *Batch) appendToArena(data []byte) (uint32, uint32) {
	start := uint32(len(b.arena))
	b.arena = append(b.arena, data...)

	return start, uint32(len(b.arena))
}

// deferSet buffers a Set operation in the arena for sorted application at Commit.
func (b *Batch) deferSet(key, value []byte) {
	keyStart, keyEnd := b.appendToArena(key)
	valueStart, valueEnd := b.appendToArena(value)
	b.entries = append(b.entries, deferredEntry{
		keyStart:   keyStart,
		keyEnd:     keyEnd,
		valueStart: valueStart,
		valueEnd:   valueEnd,
	})
}

// deferDelete buffers a Delete operation in the arena for sorted application at Commit.
func (b *Batch) deferDelete(key []byte) {
	keyStart, keyEnd := b.appendToArena(key)
	b.entries = append(b.entries, deferredEntry{
		keyStart: keyStart,
		keyEnd:   keyEnd,
		isDelete: true,
	})
}

// Cancel cancels the batch and releases resources.
func (b *Batch) Cancel() error {
	if b.committed {
		return nil
	}

	if b.batch != nil {
		return b.batch.Close()
	}

	return nil
}

// Commit commits all operations atomically with NoSync.
//
// In sorted mode, deferred point operations are sorted by key before being
// applied to the pebble.Batch. This exploits Pebble's skiplist Inserter splice
// optimization: consecutive ascending keys are inserted via a forward scan
// (O(1) amortized) instead of a full top-down traversal (O(log N) with
// cache-miss-heavy pointer chasing).
func (b *Batch) Commit() error {
	if b.committed {
		return errors.New("batch already committed")
	}

	if b.sorted && len(b.entries) > 0 {
		arena := b.arena

		slices.SortFunc(b.entries, func(a, b deferredEntry) int {
			return bytes.Compare(arena[a.keyStart:a.keyEnd], arena[b.keyStart:b.keyEnd])
		})

		for i := range b.entries {
			e := &b.entries[i]
			key := arena[e.keyStart:e.keyEnd]

			if e.isDelete {
				if err := b.batch.Delete(key, pebble.NoSync); err != nil {
					return fmt.Errorf("applying deferred delete: %w", err)
				}
			} else {
				value := arena[e.valueStart:e.valueEnd]
				if err := b.batch.Set(key, value, pebble.NoSync); err != nil {
					return fmt.Errorf("applying deferred set: %w", err)
				}
			}
		}
	}

	err := b.batch.Commit(pebble.NoSync)
	if err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	b.committed = true

	return nil
}

// Set writes a key-value pair to the batch.
// In sorted mode, the operation is deferred for sorted application at Commit.
// In default mode, the operation is applied directly to the pebble.Batch.
func (b *Batch) Set(key, value []byte, options *pebble.WriteOptions) error {
	if b.sorted {
		b.deferSet(key, value)

		return nil
	}

	return b.batch.Set(key, value, options)
}

// SetProto marshals msg and stores it under key with NoSync.
// Returns an error if the batch is already committed.
func (b *Batch) SetProto(key []byte, msg proto.Message) error {
	if b.committed {
		return errors.New("batch already committed")
	}

	data, err := b.MarshalProto(msg)
	if err != nil {
		return err
	}

	if b.sorted {
		b.deferSet(key, data)

		return nil
	}

	return b.batch.Set(key, data, pebble.NoSync)
}

// SetBytes stores raw bytes under key with NoSync.
// Returns an error if the batch is already committed.
func (b *Batch) SetBytes(key, value []byte) error {
	if b.committed {
		return errors.New("batch already committed")
	}

	if b.sorted {
		b.deferSet(key, value)

		return nil
	}

	return b.batch.Set(key, value, pebble.NoSync)
}

// DeleteKey deletes a key with NoSync.
// Returns an error if the batch is already committed.
func (b *Batch) DeleteKey(key []byte) error {
	if b.committed {
		return errors.New("batch already committed")
	}

	if b.sorted {
		b.deferDelete(key)

		return nil
	}

	return b.batch.Delete(key, pebble.NoSync)
}

// DeleteRange deletes all keys in the range [start, end).
// Always applied immediately to the pebble.Batch (not deferred) so that
// subsequent deferred Sets in the same range receive higher sequence numbers
// and correctly override the range tombstone.
func (b *Batch) DeleteRange(start, end []byte, options *pebble.WriteOptions) error {
	return b.batch.DeleteRange(start, end, options)
}

// DeleteRangeNoSync deletes all keys in [start, end) with NoSync.
// Applied immediately (see DeleteRange).
// Returns an error if the batch is already committed.
func (b *Batch) DeleteRangeNoSync(start, end []byte) error {
	if b.committed {
		return errors.New("batch already committed")
	}

	return b.batch.DeleteRange(start, end, pebble.NoSync)
}

// NewIter creates a new iterator on the store's database.
// Used by domain functions that need to read existing data during batch operations.
func (b *Batch) NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return b.store.getDB().NewIter(opts)
}
