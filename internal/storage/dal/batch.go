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

// vtDeterministicMarshaler is implemented by messages that have a
// protoc-gen-dethash generated `MarshalDeterministicVT(dAtA []byte) []byte`
// method. Used by SetProtoDeterministic so the buffer is reused across
// calls (the dethash plugin only allocates when the input buffer is too
// small to hold the marshaled output).
type vtDeterministicMarshaler interface {
	SizeVT() int
	MarshalDeterministicVT(dAtA []byte) []byte
}

// WriteSession provides atomic write operations on the store, backed by a
// pebble.Batch with NoSync writes.
//
// WriteSession is deliberately write-only: it does not expose Get / NewIter
// nor implement PebbleGetter / PebbleReader. This makes the invariant "no
// Pebble reads on the FSM hot path" structural — code that only holds a
// *WriteSession cannot read from Pebble, by the compiler.
//
// Cancel must be called if the session is not committed, to release the
// underlying batch resources.
type WriteSession struct {
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
func (b *WriteSession) MarshalProto(msg proto.Message) ([]byte, error) {
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

// OpenWriteSession creates a new write-only session bound to this store's DB.
//
// The returned session implements the write-only capability used by the FSM
// hot path. It has no read methods by design.
func (s *Store) OpenWriteSession() *WriteSession {
	return &WriteSession{
		store:       s,
		batch:       s.getDB().NewBatch(),
		KeyBuilder:  NewKeyBuilder(),
		protoBuffer: make([]byte, 0, 1024),
		CacheBuffer: make([]byte, 0, 128),
	}
}

// NewWriteSessionFromDB creates a write-only session backed by the given Pebble
// DB without a Store. Used by subsystems (e.g. readstore) that manage their own
// Pebble instance.
func NewWriteSessionFromDB(db *pebble.DB) *WriteSession {
	return &WriteSession{
		batch:       db.NewBatch(),
		KeyBuilder:  NewKeyBuilder(),
		protoBuffer: make([]byte, 0, 1024),
	}
}

// Cancel cancels the session and releases resources.
func (b *WriteSession) Cancel() error {
	if b.committed {
		return nil
	}

	if b.batch != nil {
		return b.batch.Close()
	}

	return nil
}

// Commit commits all operations atomically with NoSync.
func (b *WriteSession) Commit() error {
	if b.committed {
		return errors.New("write session already committed")
	}

	err := b.batch.Commit(pebble.NoSync)
	if err != nil {
		return fmt.Errorf("committing write session: %w", err)
	}

	b.committed = true

	return nil
}

// Set writes a key-value pair.
func (b *WriteSession) Set(key, value []byte, options *pebble.WriteOptions) error {
	return b.batch.Set(key, value, options)
}

// SetProto marshals msg and stores it under key with NoSync.
// Returns an error if the session is already committed.
func (b *WriteSession) SetProto(key []byte, msg proto.Message) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	data, err := b.MarshalProto(msg)
	if err != nil {
		return err
	}

	return b.batch.Set(key, data, pebble.NoSync)
}

// SetProtoDeterministic is the deterministic variant of SetProto: it
// marshals via MarshalDeterministicVT (map keys sorted), which is
// required for messages whose persisted bytes must be byte-identical
// across nodes — currently only auditpb.AuditEntry. Reuses
// b.protoBuffer the same way SetProto does, so the typical steady-state
// allocation count is one slice grow on the first call per session.
func (b *WriteSession) SetProtoDeterministic(key []byte, msg vtDeterministicMarshaler) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	size := msg.SizeVT()
	if cap(b.protoBuffer) < size {
		b.protoBuffer = make([]byte, 0, size)
	}

	b.protoBuffer = msg.MarshalDeterministicVT(b.protoBuffer[:0])

	return b.batch.Set(key, b.protoBuffer, pebble.NoSync)
}

// SetBytes stores raw bytes under key with NoSync.
// Returns an error if the session is already committed.
func (b *WriteSession) SetBytes(key, value []byte) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	return b.batch.Set(key, value, pebble.NoSync)
}

// DeleteKey deletes a key with NoSync.
// Returns an error if the session is already committed.
func (b *WriteSession) DeleteKey(key []byte) error {
	if b.committed {
		return errors.New("write session already committed")
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
func (b *WriteSession) SingleDeleteKey(key []byte) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	return b.batch.SingleDelete(key, pebble.NoSync)
}

// DeleteRange deletes all keys in the range [start, end).
func (b *WriteSession) DeleteRange(start, end []byte, options *pebble.WriteOptions) error {
	return b.batch.DeleteRange(start, end, options)
}

// DeleteRangeNoSync deletes all keys in [start, end) with NoSync.
// Returns an error if the session is already committed.
func (b *WriteSession) DeleteRangeNoSync(start, end []byte) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	return b.batch.DeleteRange(start, end, pebble.NoSync)
}
