package dal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/batchrepr"
	"google.golang.org/protobuf/proto"
)

// FSMDigestChain advances a rolling XXH3-128 (or otherwise per-cluster
// keyed) chain by one Raft entry: returns `H(prevHash || entryOpsBuffer)`.
// One chain link per entry — independent of how Raft groups entries into
// MsgApp batches — is what makes the rolling digest cross-node-equal at
// the same applied index.
//
// Defined here (not in processing) so dal stays free of the processing
// import; the FSM wires a concrete implementation at WriteSession open
// time via Store.OpenFSMWriteSession.
type FSMDigestChain interface {
	// Advance returns hash(prevHash || entryOps). Implementations may
	// reuse internal scratch buffers across calls. The returned slice
	// must remain valid until the next call.
	Advance(prevHash, entryOps []byte) (newHash []byte)
}

// Op kinds used by the rolling digest's per-entry buffer. Stable, never
// re-numbered: changing a value invalidates every persisted digest under
// SubGlobFSMDigest produced by older nodes.
const (
	digestOpKindSet         byte = 0x01
	digestOpKindDelete      byte = 0x02
	digestOpKindDeleteRange byte = 0x03
)

// isHashedZone reports whether a key's zone byte contributes to the
// rolling FSM digest. Limited today to the zones the diagnostic compares
// across nodes: Attributes (0x01), Cold (0x04), Idempotency (0x05).
//
// Excluded by design:
//   - Cache (0x02) and PerLedger (0x03): node-local projections / state.
//   - Global (0x06): carries the digest record itself plus per-batch
//     transient keys (LastAppliedIndex, LastAppliedTimestamp); a future
//     audit may re-include it, but only after every Global write site is
//     proven deterministic at the same applied index.
//   - ClusterTransient (0x07): FSM-tracked state that does not survive
//     cross-cluster restore.
func isHashedZone(key []byte) bool {
	if len(key) == 0 {
		return false
	}

	switch key[0] {
	case ZoneAttributes, ZoneCold, ZoneIdempotency:
		return true
	}

	return false
}

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

	// digestChain is the rolling-digest chain hook attached at session
	// open time when the FSM hot path opens the session. Non-FSM call
	// sites (lifecycle paths, backup, tests) leave it nil — their writes
	// do not contribute to the cross-node digest.
	digestChain FSMDigestChain
	// digestHash is the running rolling-digest hash, advanced by
	// AdvanceDigest. Initialised from the store's cached digest at session
	// open time (FSM hot path) and finalised into the batch by
	// CommitWithRollingDigest.
	digestHash []byte
	// entryOps accumulates the op records produced by the current Raft
	// entry's writes against the hashed zones, one self-describing record
	// per op (see mixOp / mixDeleteRange for the layout). AdvanceDigest
	// canonicalises (sorts) the records, folds them into digestHash and
	// resets it. Reused across entries to avoid per-entry alloc.
	entryOps []byte
	// entryOpBounds holds the [start, end) byte range of each op record in
	// entryOps, in append (insertion) order. AdvanceDigest sorts these
	// bounds by record bytes to produce a canonical op stream — the same
	// final Pebble state reached via a different op application order (Go
	// map-iteration order in DerivedKeyStore.Merge differs per node/run)
	// must hash identically or the FSM digest would report false
	// divergence (FSM determinism, CLAUDE.md #2). Reset alongside entryOps.
	entryOpBounds []digestOpBound
	// entryOpsCanonical is the scratch buffer AdvanceDigest assembles the
	// sorted op stream into before hashing. Reused across entries.
	entryOpsCanonical []byte
}

// digestOpBound marks one op record's byte range within WriteSession.entryOps.
type digestOpBound struct {
	start int
	end   int
}

// DeterministicEncoding reports whether this session marshals proto messages
// using the dethash plugin's sized-buffer marshaler (map keys sorted) instead
// of the historical MarshalToSizedBufferVT. The value is captured from the
// parent Store at session creation; the cluster-wide flag
// fsm_determinism_enabled (immutable post-bootstrap) controls it.
func (b *WriteSession) DeterministicEncoding() bool {
	return b.deterministicEncoding
}

// Repr returns the operation-only on-wire representation of the in-memory
// Pebble batch: the insertion-ordered byte stream that Pebble uses to replay
// the batch into the memtable / WAL, MINUS the 12-byte batch header (SeqNum
// + Count). The header carries a node-local sequence number that Pebble
// stamps post-commit; including it in the digest would make the digest
// diverge across peers even when the operation stream is identical.
//
// Stripping HeaderLen leaves the (kind, key, value) op records ONLY — those
// are deterministic across nodes by the FSM hot path's insertion-order
// contract (see the doc-block in front of WriteSet.Merge enforced by
// EN-1325). A future write site that violates the contract makes the
// digest diverge cross-node, which is exactly the signal we want.
//
// Reads from the in-memory batch buffer ONLY (never consults Pebble), so
// the "no Pebble reads on the FSM hot path" invariant is preserved: callers
// can only observe bytes they themselves wrote into this same session.
//
// Returns nil if the session is already committed (the batch's memory has
// been released) or if the batch contains zero operations.
func (b *WriteSession) Repr() []byte {
	if b.committed || b.batch == nil {
		return nil
	}

	repr := b.batch.Repr()
	if len(repr) <= batchrepr.HeaderLen {
		return nil
	}

	return repr[batchrepr.HeaderLen:]
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
//
// The returned session does NOT participate in the rolling FSM digest —
// callers on the FSM hot path that need digest chaining must use
// OpenFSMWriteSession.
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

// OpenFSMWriteSession creates a write-only session wired to the rolling
// cross-node FSM digest chain. Caller MUST be the FSM hot path:
//
//   - chain is the per-entry hash advance (typically a thin wrapper around
//     processing.HashGenerator that owns its own scratch buffer).
//   - The session captures the store's PENDING rolling digest at open
//     time and uses it as the chain seed for the first entry. The pending
//     digest advances at prepare time (RecordPendingDigest), so under
//     pipelining a batch opened while a previous batch is still committing
//     chains from that previous batch's hash rather than dropping it.
//   - The FSM is contractually required to call AdvanceDigest at the end
//     of every entry it applies, RecordPendingDigest once the batch's
//     chain is fully advanced (before returning the prepared batch), and
//     CommitWithRollingDigest at end of batch (instead of plain Commit).
//
// Open-time read is from the in-memory cache on Store; the apply path
// itself never touches Pebble, preserving invariant #3 (no Pebble reads
// on the hot path).
//
// When the store has deterministic encoding off, or chain is nil, the
// returned session is byte-equivalent to OpenWriteSession (no digest
// instrumentation, no per-entry buffer allocation). The FSM can always
// call OpenFSMWriteSession unconditionally and let this short-circuit
// handle the disabled path.
func (s *Store) OpenFSMWriteSession(chain FSMDigestChain) *WriteSession {
	sess := s.OpenWriteSession()
	if !s.deterministicEncoding || chain == nil {
		return sess
	}

	_, hash := s.RollingDigestPending()
	sess.digestChain = chain
	sess.digestHash = hash
	sess.entryOps = make([]byte, 0, 1024)

	return sess
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
//
// Sessions opened via OpenFSMWriteSession MUST use CommitWithRollingDigest
// instead — Commit on those sessions leaves the rolling digest unpersisted
// even though it has been advanced in memory, breaking the next session's
// chain seed. We refuse it loudly rather than committing silently.
func (b *WriteSession) Commit() error {
	if b.committed {
		return errors.New("write session already committed")
	}

	if b.digestChain != nil {
		return errors.New("invariant: WriteSession opened via OpenFSMWriteSession must use CommitWithRollingDigest")
	}

	err := b.batch.Commit(pebble.NoSync)
	if err != nil {
		return fmt.Errorf("committing write session: %w", err)
	}

	b.committed = true

	return nil
}

// AdvanceDigest canonicalises the per-entry op records, folds them into the
// rolling hash and resets the buffers. Must be called by the FSM after each
// applied entry so the chain advances by one link per Raft entry —
// independent of how many entries Raft groups into the surrounding MsgApp
// batch. No-op on sessions that don't participate in the digest chain.
//
// Canonicalisation: op records are sorted by their raw bytes before hashing.
// The order in which an entry's writes hit the batch is NOT deterministic
// across nodes — DerivedKeyStore.Merge drains its overlay by ranging Go maps,
// whose iteration order is randomised per process. Two nodes applying the
// same entry reach byte-identical Pebble state (Set/Delete on distinct keys
// is order-independent) but would emit their op records in different orders;
// hashing the raw insertion order would then report false FSM divergence
// (CLAUDE.md #2). Sorting the records yields a total order keyed on
// (kind, key, value) — unique per op since keys within an entry are distinct
// — so the folded bytes match across nodes.
func (b *WriteSession) AdvanceDigest() {
	if b.digestChain == nil {
		return
	}

	canonical := b.entryOps
	if len(b.entryOpBounds) > 1 {
		slices.SortFunc(b.entryOpBounds, func(x, y digestOpBound) int {
			return bytes.Compare(b.entryOps[x.start:x.end], b.entryOps[y.start:y.end])
		})

		b.entryOpsCanonical = b.entryOpsCanonical[:0]
		for _, bnd := range b.entryOpBounds {
			b.entryOpsCanonical = append(b.entryOpsCanonical, b.entryOps[bnd.start:bnd.end]...)
		}
		canonical = b.entryOpsCanonical
	}

	b.digestHash = append(b.digestHash[:0:0], b.digestChain.Advance(b.digestHash, canonical)...)
	b.entryOps = b.entryOps[:0]
	b.entryOpBounds = b.entryOpBounds[:0]
}

// RecordPendingDigest publishes the batch's fully-advanced rolling hash to
// the store's pending-digest slot so the NEXT FSM WriteSession chains from
// it — even if this batch has not committed yet. Called by the FSM at the
// end of the prepare phase, after every entry's AdvanceDigest and before the
// prepared batch is handed to the pipelined committer.
//
// Without this the next OpenFSMWriteSession would seed from the committed
// rolling digest (updated only by CommitWithRollingDigest), which under
// pipelining still reflects the batch BEFORE this one — silently dropping
// this batch from the chain at the batch boundary. No-op on sessions that
// don't participate in the digest chain.
//
// appliedIndex is the highest Raft index folded into this batch; it is
// stored alongside the hash purely for observability (the seed the next
// session reads is the hash, the index is diagnostic).
func (b *WriteSession) RecordPendingDigest(appliedIndex uint64) {
	if b.digestChain == nil || b.store == nil {
		return
	}

	b.store.SetPendingRollingDigest(appliedIndex, b.digestHash)
}

// CommitWithRollingDigest finalises the FSM-side hot-path commit: writes
// the (appliedIndex, rolling hash) tuple under SubGlobFSMDigest as the
// last op in the batch, commits the batch atomically, and on success
// updates the in-memory cache on Store so the next session reads the
// fresh seed. Returns the persisted hash bytes for the caller's bookkeeping.
//
// Sessions opened via OpenWriteSession (no chain attached) accept this
// method too, but it degenerates to plain Commit and returns ZeroFSMDigest
// — useful when the FSM determinism flag is OFF cluster-wide and the
// hot path nonetheless wants a single commit entry point.
//
// Invariant: every Raft entry the FSM applied through this session must
// have been followed by an AdvanceDigest call. If entryOps is non-empty
// at commit time the chain link for the last entry has not been folded
// in — we refuse to commit a silently-divergent record.
func (b *WriteSession) CommitWithRollingDigest(appliedIndex uint64) ([]byte, error) {
	if b.committed {
		return nil, errors.New("write session already committed")
	}

	if b.digestChain == nil {
		return ZeroFSMDigest, b.commitNoDigest()
	}

	if len(b.entryOps) > 0 {
		return nil, fmt.Errorf(
			"invariant: %d unflushed digest entry ops at commit (FSM forgot AdvanceDigest)",
			len(b.entryOps),
		)
	}

	record, err := EncodeFSMDigest(appliedIndex, b.digestHash)
	if err != nil {
		return nil, fmt.Errorf("encoding rolling fsm digest: %w", err)
	}

	if err := b.batch.Set(fsmDigestKey, record, pebble.NoSync); err != nil {
		return nil, fmt.Errorf("staging rolling fsm digest: %w", err)
	}

	if err := b.commitNoDigest(); err != nil {
		return nil, err
	}

	if b.store != nil {
		b.store.SetRollingDigest(appliedIndex, b.digestHash)
	}

	return b.digestHash, nil
}

// commitNoDigest is the Commit body without the chain guard (the
// CommitWithRollingDigest path has already validated the chain state and
// must be allowed to commit even when digestChain is set).
func (b *WriteSession) commitNoDigest() error {
	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing write session: %w", err)
	}

	b.committed = true

	return nil
}

// mixOp folds a single (kind, key, value) op into the per-entry digest
// buffer. Filters out writes whose zone is not part of the cross-node
// digest contract — those are either node-local projections (Cache,
// PerLedger), the digest record itself (Global / SubGlobFSMDigest), or
// transient (ClusterTransient).
//
// The encoding is canonical (uvarint lengths) so the bytes mixed in are
// independent of any Pebble batch framing quirks.
func (b *WriteSession) mixOp(kind byte, key, value []byte) {
	if b.digestChain == nil || !isHashedZone(key) {
		return
	}

	start := len(b.entryOps)
	b.entryOps = append(b.entryOps, kind)
	b.entryOps = binary.AppendUvarint(b.entryOps, uint64(len(key)))
	b.entryOps = append(b.entryOps, key...)
	b.entryOps = binary.AppendUvarint(b.entryOps, uint64(len(value)))
	b.entryOps = append(b.entryOps, value...)
	b.entryOpBounds = append(b.entryOpBounds, digestOpBound{start: start, end: len(b.entryOps)})
}

// mixDeleteRange folds a DeleteRange op into the per-entry digest buffer
// using both endpoints. Only mixed when start AND end have hashed-zone
// prefix bytes — a range that crosses zones would imply an unusual write
// pattern; we conservatively skip it from the digest in that case rather
// than mixing partial info.
func (b *WriteSession) mixDeleteRange(start, end []byte) {
	if b.digestChain == nil {
		return
	}

	if !isHashedZone(start) || len(end) == 0 || !isHashedZone(end) {
		return
	}

	recStart := len(b.entryOps)
	b.entryOps = append(b.entryOps, digestOpKindDeleteRange)
	b.entryOps = binary.AppendUvarint(b.entryOps, uint64(len(start)))
	b.entryOps = append(b.entryOps, start...)
	b.entryOps = binary.AppendUvarint(b.entryOps, uint64(len(end)))
	b.entryOps = append(b.entryOps, end...)
	b.entryOpBounds = append(b.entryOpBounds, digestOpBound{start: recStart, end: len(b.entryOps)})
}

// Set writes a key-value pair.
func (b *WriteSession) Set(key, value []byte, options *pebble.WriteOptions) error {
	b.mixOp(digestOpKindSet, key, value)

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

	b.mixOp(digestOpKindSet, key, data)

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
	b.mixOp(digestOpKindSet, key, b.protoBuffer)

	return b.batch.Set(key, b.protoBuffer, pebble.NoSync)
}

// SetBytes stores raw bytes under key with NoSync.
// Returns an error if the session is already committed.
func (b *WriteSession) SetBytes(key, value []byte) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	b.mixOp(digestOpKindSet, key, value)

	return b.batch.Set(key, value, pebble.NoSync)
}

// DeleteKey deletes a key with NoSync.
// Returns an error if the session is already committed.
func (b *WriteSession) DeleteKey(key []byte) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	b.mixOp(digestOpKindDelete, key, nil)

	return b.batch.Delete(key, pebble.NoSync)
}

// SingleDeleteKey deletes a key that was written exactly once (single SET) with NoSync.
// Unlike DeleteKey, the tombstone is eliminated as soon as it meets the matching SET
// during compaction at any level, avoiding tombstone accumulation in the LSM.
//
// SAFETY: Using SingleDelete on a key that was written more than once (multiple SETs)
// produces undefined behavior — the key may reappear after compaction.
// Only use for keys with a guaranteed write-once / delete-once lifecycle.
//
// SingleDelete and Delete are equivalent from the cross-node digest's
// perspective: both produce a deletion of `key`. The digest mixes them
// under the same op kind so a future migration between the two variants
// at a given site is transparent to the chain.
func (b *WriteSession) SingleDeleteKey(key []byte) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	b.mixOp(digestOpKindDelete, key, nil)

	return b.batch.SingleDelete(key, pebble.NoSync)
}

// DeleteRange deletes all keys in the range [start, end).
func (b *WriteSession) DeleteRange(start, end []byte, options *pebble.WriteOptions) error {
	b.mixDeleteRange(start, end)

	return b.batch.DeleteRange(start, end, options)
}

// DeleteRangeNoSync deletes all keys in [start, end) with NoSync.
// Returns an error if the session is already committed.
func (b *WriteSession) DeleteRangeNoSync(start, end []byte) error {
	if b.committed {
		return errors.New("write session already committed")
	}

	b.mixDeleteRange(start, end)

	return b.batch.DeleteRange(start, end, pebble.NoSync)
}
