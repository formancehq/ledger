package dal

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"
)

// vtSizedBufferMarshaler is implemented by vtprotobuf-generated messages.
type vtSizedBufferMarshaler interface {
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
}

// Batch provides atomic operations on the store using a pebble.Batch with NoSync.
//
// Point operations (Set, Delete) are written directly to the underlying
// pebble.Batch. Range operations (DeleteRange) are also applied immediately.
//
// Cancel must be called if the batch is not committed to release resources.
type Batch struct {
	store          *Store
	batch          *pebble.Batch
	KeyBuilder     *KeyBuilder
	protoBuffer    []byte
	CacheBuffer    []byte // reusable buffer for 0xFF cache zone writes (tag+value)
	committed      bool
	marshalOptions proto.MarshalOptions
}

// MarshalProto marshals a proto message using vtprotobuf when available,
// falling back to standard MarshalAppend otherwise.
//
// Calls SizeVT once and uses MarshalToSizedBufferVT directly, avoiding the
// double SizeVT that MarshalToVT would do internally.
func (b *Batch) MarshalProto(msg proto.Message) ([]byte, error) {
	if m, ok := msg.(vtSizedBufferMarshaler); ok {
		size := m.SizeVT()
		if cap(b.protoBuffer) >= size {
			b.protoBuffer = b.protoBuffer[:size]
		} else {
			b.protoBuffer = make([]byte, size)
		}

		n, err := m.MarshalToSizedBufferVT(b.protoBuffer)

		return b.protoBuffer[size-n:], err
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
func (b *Batch) Commit() error {
	if b.committed {
		return errors.New("batch already committed")
	}

	err := b.batch.Commit(pebble.NoSync)
	if err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	b.committed = true

	return nil
}

// Set writes a key-value pair to the batch.
func (b *Batch) Set(key, value []byte, options *pebble.WriteOptions) error {
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

	return b.batch.Set(key, data, pebble.NoSync)
}

// SetBytes stores raw bytes under key with NoSync.
// Returns an error if the batch is already committed.
func (b *Batch) SetBytes(key, value []byte) error {
	if b.committed {
		return errors.New("batch already committed")
	}

	return b.batch.Set(key, value, pebble.NoSync)
}

// DeleteKey deletes a key with NoSync.
// Returns an error if the batch is already committed.
func (b *Batch) DeleteKey(key []byte) error {
	if b.committed {
		return errors.New("batch already committed")
	}

	return b.batch.Delete(key, pebble.NoSync)
}

// SingleDeleteKey deletes a key that was written exactly once (single SET) with NoSync.
// Unlike DeleteKey, the tombstone is eliminated as soon as it meets the matching SET
// during compaction at any level, avoiding tombstone accumulation in the LSM.
//
// SAFETY: Using SingleDelete on a key that was written more than once (multiple SETs)
// produces undefined behavior — the key may reappear after compaction.
// Only use for keys with a guaranteed write-once / delete-once lifecycle.
func (b *Batch) SingleDeleteKey(key []byte) error {
	if b.committed {
		return errors.New("batch already committed")
	}

	return b.batch.SingleDelete(key, pebble.NoSync)
}

// DeleteRange deletes all keys in the range [start, end).
func (b *Batch) DeleteRange(start, end []byte, options *pebble.WriteOptions) error {
	return b.batch.DeleteRange(start, end, options)
}

// DeleteRangeNoSync deletes all keys in [start, end) with NoSync.
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
