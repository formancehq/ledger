package dal

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"
)

// vtSizedBufferMarshaler is implemented by every vtprotobuf-generated message.
// MarshalToSizedBufferVT writes into a caller-supplied buffer (no allocation)
// and is the historical fast path for SetProto in non-deterministic mode.
type vtSizedBufferMarshaler interface {
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
}

// vtSizedBufferDeterministicMarshaler is implemented by messages that the
// protoc-gen-dethash plugin emits a `MarshalToSizedBufferDeterministicVT`
// method for. The plugin emits it ONLY for messages whose transitive shape
// contains a map (the marshaler sorts the map keys before writing). For
// map-free messages the plugin's dethash output is wire-identical to the
// regular MarshalToSizedBufferVT, so we fall back to the regular one (the
// determinism comes for free from the absence of maps).
//
// Using the SizedBuffer flavor (vs the append-style MarshalDeterministicVT)
// keeps the marshal in-place into WriteSession.protoBuffer — zero allocation
// in steady state.
type vtSizedBufferDeterministicMarshaler interface {
	SizeVT() int
	MarshalToSizedBufferDeterministicVT([]byte) (int, error)
}

// vtAppendDeterministicMarshaler is the append-style deterministic marshaler
// (the plugin emits both signatures for messages with maps). Used by
// SetProtoDeterministic where we want a stable contract on the
// per-AuditEntry path (audit hash chain is built from these bytes).
type vtAppendDeterministicMarshaler interface {
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
	store                 *Store
	batch                 *pebble.Batch
	KeyBuilder            *KeyBuilder
	protoBuffer           []byte
	CacheBuffer           []byte // reusable buffer for 0xFF cache zone writes (tag+value)
	committed             bool
	marshalOptions        proto.MarshalOptions
	deterministicEncoding bool // captured from the store at OpenWriteSession time
}

// DeterministicEncoding reports whether this session marshals proto messages
// using the dethash plugin's sized-buffer marshaler (map keys sorted) instead
// of the historical MarshalToSizedBufferVT. The value is captured from the
// parent Store at session creation; the cluster-wide flag
// fsm_determinism_enabled (immutable post-bootstrap) controls it.
func (b *WriteSession) DeterministicEncoding() bool {
	return b.deterministicEncoding
}

// Repr returns the on-wire representation of the in-memory Pebble batch:
// the insertion-ordered byte stream that Pebble itself uses to replay the
// batch into the memtable / WAL. Used by the cross-node FSM digest to hash
// the batch contents in one shot.
//
// The FSM hot path's insertion order is deterministic by construction (see
// the doc-block in front of WriteSet.Merge enforced by EN-1325), so
// Repr() is byte-identical across nodes for the same set of FSM mutations.
// Hashing it directly is faster than re-iterating + sorting + re-formatting,
// AND exposes any future regression of the insertion-order contract as a
// digest divergence — which is exactly the signal we want.
//
// Reads from the in-memory batch buffer ONLY (never consults Pebble), so
// the "no Pebble reads on the FSM hot path" invariant is preserved: callers
// can only observe bytes they themselves wrote into this same session.
//
// Returns nil if the session is already committed (the batch's memory has
// been released).
func (b *WriteSession) Repr() []byte {
	if b.committed || b.batch == nil {
		return nil
	}

	return b.batch.Repr()
}

// MarshalProto marshals a proto message using vtprotobuf when available,
// falling back to standard MarshalAppend otherwise.
//
// When the parent store has DeterministicEncoding=true:
//   - Messages with maps: route through MarshalToSizedBufferDeterministicVT
//     (sorts the map keys before writing) — generated only for messages whose
//     shape transitively contains a map<>.
//   - Messages without maps: fall through to MarshalToSizedBufferVT — the
//     wire output is byte-identical to the dethash output because there are
//     no maps to sort, so the determinism is free.
//
// Both deterministic paths reuse b.protoBuffer (in-place marshal), keeping
// the cost of the flag down to ONLY the marshal-time map-key sort when one
// is actually present. No allocation in steady state.
//
// When DeterministicEncoding=false, the same MarshalToSizedBufferVT path is
// used unconditionally (historical default).
func (b *WriteSession) MarshalProto(msg proto.Message) ([]byte, error) {
	if b.deterministicEncoding {
		// Map-bearing messages: in-place dethash marshal (sort + write).
		if m, ok := msg.(vtSizedBufferDeterministicMarshaler); ok {
			size := m.SizeVT()
			if cap(b.protoBuffer) >= size {
				b.protoBuffer = b.protoBuffer[:size]
			} else {
				b.protoBuffer = make([]byte, size)
			}

			n, err := m.MarshalToSizedBufferDeterministicVT(b.protoBuffer)

			return b.protoBuffer[size-n:], err
		}

		// Map-free vt messages: fall through to the standard sized-buffer
		// path. No maps means MarshalToSizedBufferVT is already byte-equivalent
		// to the deterministic output, so we save an extra alloc by reusing
		// the same in-place writer.
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

		// Non-vt fallback: use protobuf's deterministic marshaler.
		opts := b.marshalOptions
		opts.Deterministic = true

		return opts.MarshalAppend(b.protoBuffer[:0], msg)
	}

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
		store:                 s,
		batch:                 s.getDB().NewBatch(),
		KeyBuilder:            NewKeyBuilder(),
		protoBuffer:           make([]byte, 0, 1024),
		CacheBuffer:           make([]byte, 0, 128),
		deterministicEncoding: s.deterministicEncoding,
	}
}

// NewWriteSessionFromDB creates a write-only session backed by the given Pebble
// DB without a Store. Used by subsystems (e.g. readstore) that manage their own
// Pebble instance. Sessions opened via this constructor do NOT use deterministic
// encoding — they are not part of the FSM hot path and do not feed the
// cross-node digest.
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
// across nodes INDEPENDENTLY of the cluster-wide fsm_determinism_enabled
// flag — currently only auditpb.AuditEntry (the audit hash chain is built
// from these bytes and would diverge on any node booted with the flag OFF).
// Reuses b.protoBuffer the same way SetProto does, so the typical
// steady-state allocation count is one slice grow on the first call per
// session.
func (b *WriteSession) SetProtoDeterministic(key []byte, msg vtAppendDeterministicMarshaler) error {
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
