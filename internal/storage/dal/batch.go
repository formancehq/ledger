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
// A WriteSession reaches a terminal state either via a successful Commit (the
// batch is applied, finalised exactly once and returned to Pebble's pool) or
// via Cancel (an unfinished batch is closed at most once). After a terminal
// state every mutator returns a documented error and never touches the
// released batch.
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

// checkActive returns the terminal-state error if the session has already been
// committed or cancelled; otherwise nil. It is the single guard shared by every
// mutator so no operation can reach the underlying Pebble batch after it has
// been released back to Pebble's pool.
func (b *WriteSession) checkActive() error {
	if b.committed {
		return errors.New("write session already committed")
	}

	if b.batch == nil {
		return errors.New("write session already cancelled")
	}

	return nil
}

// Cancel cancels the session and releases resources. It is idempotent: once the
// session has reached a terminal state (committed or cancelled), further calls
// are no-ops.
func (b *WriteSession) Cancel() error {
	if b.committed || b.batch == nil {
		return nil
	}

	err := b.batch.Close()
	b.batch = nil

	return err
}

// Commit commits all operations atomically with NoSync. On success the owned
// Pebble batch is finalised exactly once: it is closed and released back to
// Pebble's pool (or the release is deferred while the WAL commit pipeline still
// holds a reference under NoSync), and the session enters the committed terminal
// state. If the underlying commit fails, the batch remains owned by the session
// so the caller can Cancel it to release resources; a failed commit is not
// described as rolled back. If the batch cannot be finalised after a successful
// commit, the error is returned and the session still enters the committed
// terminal state, since the data was already applied.
func (b *WriteSession) Commit() error {
	if err := b.checkActive(); err != nil {
		return err
	}

	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing write session: %w", err)
	}

	b.committed = true

	if err := b.batch.Close(); err != nil {
		b.batch = nil

		return fmt.Errorf("finalizing write session batch: %w", err)
	}
	b.batch = nil

	return nil
}

// Set writes a key-value pair.
// Returns an error if the session has reached a terminal state.
func (b *WriteSession) Set(key, value []byte, options *pebble.WriteOptions) error {
	if err := b.checkActive(); err != nil {
		return err
	}

	return b.batch.Set(key, value, options)
}

// SetProto marshals msg and stores it under key with NoSync.
// Returns an error if the session has reached a terminal state.
func (b *WriteSession) SetProto(key []byte, msg proto.Message) error {
	if err := b.checkActive(); err != nil {
		return err
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
	if err := b.checkActive(); err != nil {
		return err
	}

	size := msg.SizeVT()
	if cap(b.protoBuffer) < size {
		b.protoBuffer = make([]byte, 0, size)
	}

	b.protoBuffer = msg.MarshalDeterministicVT(b.protoBuffer[:0])

	return b.batch.Set(key, b.protoBuffer, pebble.NoSync)
}

// SetBytes stores raw bytes under key with NoSync.
// Returns an error if the session has reached a terminal state.
func (b *WriteSession) SetBytes(key, value []byte) error {
	if err := b.checkActive(); err != nil {
		return err
	}

	return b.batch.Set(key, value, pebble.NoSync)
}

// DeleteKey deletes a key with NoSync.
// Returns an error if the session has reached a terminal state.
func (b *WriteSession) DeleteKey(key []byte) error {
	if err := b.checkActive(); err != nil {
		return err
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
	if err := b.checkActive(); err != nil {
		return err
	}

	return b.batch.SingleDelete(key, pebble.NoSync)
}

// DeleteRange deletes all keys in the range [start, end).
// Returns an error if the session has reached a terminal state.
func (b *WriteSession) DeleteRange(start, end []byte, options *pebble.WriteOptions) error {
	if err := b.checkActive(); err != nil {
		return err
	}

	return b.batch.DeleteRange(start, end, options)
}

// DeleteRangeNoSync deletes all keys in [start, end) with NoSync.
// Returns an error if the session has reached a terminal state.
func (b *WriteSession) DeleteRangeNoSync(start, end []byte) error {
	if err := b.checkActive(); err != nil {
		return err
	}

	return b.batch.DeleteRange(start, end, pebble.NoSync)
}
