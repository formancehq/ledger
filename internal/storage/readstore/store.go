package readstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/pebblecfg"
)

// Config is the Pebble configuration for the read index store.
// It uses the same tunables as the primary store (pebblecfg.Config).
type Config = pebblecfg.Config

// DefaultConfig returns the default Pebble configuration for the read index.
// These defaults are intentionally smaller than the primary DAL store because
// the read index is a derived view that can be rebuilt from the Raft log.
func DefaultConfig() Config {
	return Config{
		MemTableSize:                64 << 20, // 64MB
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       12,
		LBaseMaxBytes:               512 << 20, // 512MB
		CacheSize:                   64 << 20,  // 64MB
		TargetFileSize:              64 << 20,  // 64MB
		BytesPerSync:                512 << 10, // 512KB
		MaxConcurrentCompactions:    1,
		Compression:                 pebblecfg.DefaultLevelCompression(),
	}
}

// Store wraps a Pebble database for the read-side inverted indexes.
// It is safe for concurrent use: Pebble supports concurrent readers
// and writers without a global write lock.
type Store struct {
	db     *pebble.DB
	logger logging.Logger
	dir    string

	// progressMu and progressCond allow callers to wait until the indexed
	// sequence reaches a target value. The index builder calls
	// NotifyProgress after each WriteProgress to wake up waiters.
	progressMu      sync.Mutex
	progressCond    *sync.Cond
	auditDisabled   bool
	auditRebuilding bool
	auditGeneration uint64

	// readOnly marks a store opened via OpenReadOnly — a frozen view (query
	// checkpoint) whose fold cursor will never advance, so freshness waits
	// are meaningless against it.
	readOnly bool

	// leases tracks the pinned sequences of live reads so the event GC never
	// reclaims history a pinned reader could still resolve (see read_lease.go
	// and event_gc.go). Nil on frozen stores — no GC runs against them.
	leases *LeaseRegistry
}

// Leases returns the read-lease registry gating the event GC.
func (s *Store) Leases() *LeaseRegistry { return s.leases }

// Frozen reports whether this store is an immutable read-only view (a query
// checkpoint) rather than the live, builder-fed read index.
func (s *Store) Frozen() bool { return s.readOnly }

// New opens or creates a Pebble database at the given directory for the read index.
func New(dir string, logger logging.Logger, cfg Config) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating read store directory: %w", err)
	}

	dbPath := filepath.Join(dir, "readindex")

	var fileSize int64
	if info, _ := os.Stat(dbPath); info != nil {
		fileSize = info.Size()
	}

	logger.WithFields(map[string]any{
		"path":     dbPath,
		"fileSize": fileSize,
	}).Infof("Opening Pebble read index")

	openStart := time.Now()

	cache := pebble.NewCache(cfg.CacheSize)
	defer cache.Unref()

	opts := &pebble.Options{
		Logger:             dal.NewPebbleLogger(logger),
		FormatMajorVersion: pebble.FormatNewest,
		// Custom comparer: splits keys at [prefix][ledger\x00] boundary
		// so bloom filters are built on ledger-scoped prefixes, enabling
		// SeekPrefixGE to skip SSTables that don't contain the target ledger.
		Comparer: ReadStoreComparer,
		// The read index is a derived view rebuilt from the Raft log.
		// We can safely disable WAL: on crash the index builder simply
		// replays from its last progress cursor.
		DisableWAL:                  true,
		MemTableSize:                cfg.MemTableSize,
		MemTableStopWritesThreshold: cfg.MemTableStopWritesThreshold,
		L0CompactionThreshold:       cfg.L0CompactionThreshold,
		L0StopWritesThreshold:       cfg.L0StopWritesThreshold,
		LBaseMaxBytes:               cfg.LBaseMaxBytes,
		BytesPerSync:                cfg.BytesPerSync,
		CompactionConcurrencyRange: func() (int, int) {
			n := cfg.MaxConcurrentCompactions

			return n, n
		},
		Cache:           cache,
		TargetFileSizes: cfg.BuildTargetFileSizes(),
		Levels:          cfg.BuildLevels(),
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening Pebble read index: %w", err)
	}

	m := db.Metrics()
	logger.WithFields(map[string]any{
		"duration":          time.Since(openStart).String(),
		"l0FileCount":       m.Levels[0].TablesCount,
		"l0Size":            m.Levels[0].TablesSize,
		"l1FileCount":       m.Levels[1].TablesCount,
		"l1Size":            m.Levels[1].TablesSize,
		"memTableCount":     m.MemTable.Count,
		"memTableSize":      m.MemTable.Size,
		"compactionCount":   m.Compact.Count,
		"compactionEstDebt": m.Compact.EstimatedDebt,
		"totalLevelsSize":   m.DiskSpaceUsage(),
	}).Infof("Pebble read index opened — LSM state")

	s := &Store{
		db:     db,
		logger: logger.WithFields(map[string]any{"cmp": "read-store"}),
		dir:    dir,
		leases: NewLeaseRegistry(),
	}
	s.progressCond = sync.NewCond(&s.progressMu)

	return s, nil
}

// OpenReadOnly opens a Pebble read index at dirPath in read-only mode.
// The caller must call Close() when done.
func OpenReadOnly(dirPath string, logger logging.Logger) (*Store, error) {
	db, err := pebble.Open(dirPath, &pebble.Options{
		Logger:   dal.NewPebbleLogger(logger),
		Comparer: ReadStoreComparer,
		ReadOnly: true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening read-only Pebble read index at %s: %w", dirPath, err)
	}

	s := &Store{
		db:       db,
		logger:   logger.WithFields(map[string]any{"cmp": "read-store-readonly"}),
		dir:      dirPath,
		readOnly: true,
		leases:   NewLeaseRegistry(),
	}
	s.progressCond = sync.NewCond(&s.progressMu)

	return s, nil
}

// CreateCheckpoint creates a Pebble checkpoint of the read index at destDir.
// The read index has WAL disabled, so committed batches may exist only in a
// memtable. Flush it first: otherwise Pebble has neither an SST nor a WAL file
// to link and a ready checkpoint can silently omit the progress certificates
// and projection rows that its creator just waited for.
func (s *Store) CreateCheckpoint(destDir string) error {
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("flushing read index before checkpoint: %w", err)
	}

	return s.db.Checkpoint(destDir)
}

// Close closes the underlying Pebble database.
func (s *Store) Close() error {
	return s.db.Close()
}

// DB returns the underlying Pebble database for creating batches.
func (s *Store) DB() *pebble.DB {
	return s.db
}

// NewBatch creates a dal.WriteSession backed by the read store's Pebble DB.
func (s *Store) NewBatch() *dal.WriteSession {
	return dal.NewWriteSessionFromDB(s.db)
}

// NewSnapshot returns a consistent snapshot for reads.
// The caller must call snap.Close() when done.
func (s *Store) NewSnapshot() *pebble.Snapshot {
	return s.db.NewSnapshot()
}

// Path returns the directory of the read index.
func (s *Store) Path() string {
	return s.dir
}

// ReadProgress returns the last indexed log sequence from the progress key.
// Returns 0 if no progress has been recorded.
func (s *Store) ReadProgress() (uint64, error) {
	return progressCursor.Read(s.db)
}

// ReadProgressFrom is the snapshot-aware variant of ReadProgress: it reads
// from an arbitrary PebbleGetter (typically a *pebble.Snapshot taken via
// NewSnapshot()) instead of the live DB, so multi-step readers can pin a
// consistent view.
func (s *Store) ReadProgressFrom(reader dal.PebbleGetter) (uint64, error) {
	return progressCursor.Read(reader)
}

// WriteProgress stores the last indexed log sequence.
func (s *Store) WriteProgress(batch *dal.WriteSession, sequence uint64) error {
	return progressCursor.Write(batch, sequence)
}

// ReadRaftProgress returns the Raft horizon certified by the normal read
// projection. The native log cursor remains the fold/reclamation position.
func (s *Store) ReadRaftProgress() (uint64, error) {
	return readRaftCursor.Read(s.db)
}

// ReadRaftProgressFrom reads the normal projection certificate from a pinned
// snapshot, so the certificate and index rows come from one Pebble view.
func (s *Store) ReadRaftProgressFrom(reader dal.PebbleGetter) (uint64, error) {
	return readRaftCursor.Read(reader)
}

// WriteRaftProgress publishes a normal-projection causal certificate. It must
// be committed atomically with the final writes for that target.
func (s *Store) WriteRaftProgress(batch *dal.WriteSession, appliedIndex uint64) error {
	return readRaftCursor.Write(batch, appliedIndex)
}

// LastIndexedSequence returns the last indexed log sequence (read-only).
func (s *Store) LastIndexedSequence() (uint64, error) {
	return s.ReadProgress()
}

// LastIndexedSequenceFrom is the snapshot-aware variant. Callers that hold
// a snapshot for a multi-step read must use this form so the value stays
// pinned to the snapshot rather than advancing under their feet.
func (s *Store) LastIndexedSequenceFrom(reader dal.PebbleGetter) (uint64, error) {
	return s.ReadProgressFrom(reader)
}

// NotifyProgress wakes goroutines waiting for native or certified projection
// progress and for checkpoint readiness. Call it only after the corresponding
// progress write has committed or the checkpoint marker has been materialized.
//
// The broadcast is issued while holding progressMu: a waiter checks its
// condition and calls cond.Wait() under the same lock, and Wait atomically
// releases the lock only once it is parked. Taking progressMu here therefore
// serializes against that window — the broadcast either lands before the waiter
// locks (it will re-check the condition when it acquires the lock) or after it
// has parked (it will be woken). Without the lock, a broadcast between the
// condition check and Wait() would be missed until the next notification.
func (s *Store) NotifyProgress() {
	s.progressMu.Lock()
	s.progressCond.Broadcast()
	s.progressMu.Unlock()
}

// SetAuditProjectionState records node-local operational readiness. It never
// participates in Raft apply; it only prevents a causal progress certificate
// from being mistaken for readiness while the local audit projection is
// disabled or being rebuilt.
func (s *Store) SetAuditProjectionState(disabled, rebuilding bool) {
	s.progressMu.Lock()
	if s.auditDisabled != disabled || s.auditRebuilding != rebuilding {
		s.auditGeneration++
	}
	s.auditDisabled = disabled
	s.auditRebuilding = rebuilding
	s.progressCond.Broadcast()
	s.progressMu.Unlock()
}

// AuditProjectionState returns the local audit projection lifecycle state.
func (s *Store) AuditProjectionState() (disabled, rebuilding bool) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	return s.auditDisabled, s.auditRebuilding
}

// AuditProjectionStateWithGeneration returns lifecycle state together with a
// process-local generation that changes on every readiness transition. A
// checkpoint captures the generation after its progress wait and verifies it
// again when publishing .ready, so a concurrent rebuild cannot certify a
// snapshot taken from a reset projection.
func (s *Store) AuditProjectionStateWithGeneration() (disabled, rebuilding bool, generation uint64) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	return s.auditDisabled, s.auditRebuilding, s.auditGeneration
}

// MarkCheckpointReadyAtAuditGeneration publishes the marker only if the audit
// projection remained ready in the generation captured before materializing
// the checkpoint. The state lock closes the final check-to-marker race with
// SetAuditProjectionState.
func (s *Store) MarkCheckpointReadyAtAuditGeneration(dir string, generation uint64) (bool, error) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	if s.auditDisabled || s.auditRebuilding || s.auditGeneration != generation {
		return false, nil
	}

	return true, MarkCheckpointReady(dir)
}

// ReadAppliedProposalProgress returns the last consumed AppliedProposal
// sequence. Returns 0 if no progress has been recorded yet.
func (s *Store) ReadAppliedProposalProgress() (uint64, error) {
	return appliedProposalCursor.Read(s.db)
}

// WriteAppliedProposalProgress stores the last consumed AppliedProposal sequence.
func (s *Store) WriteAppliedProposalProgress(batch *dal.WriteSession, sequence uint64) error {
	return appliedProposalCursor.Write(batch, sequence)
}

// WriteBackfillProgress stores a backfill cursor.
func (s *Store) WriteBackfillProgress(batch *dal.WriteSession, key []byte, cursor uint64) error {
	prefix := BackfillKeyPrefix()
	fullKey := make([]byte, len(prefix)+len(key))
	copy(fullKey, prefix)
	copy(fullKey[len(prefix):], key)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cursor)

	return batch.SetBytes(fullKey, buf[:])
}

// ReadBackfillProgress reads a backfill cursor.
// Returns (cursor, true) if found, (0, false) if the key does not exist.
func (s *Store) ReadBackfillProgress(key []byte) (uint64, bool) {
	prefix := BackfillKeyPrefix()
	fullKey := make([]byte, len(prefix)+len(key))
	copy(fullKey, prefix)
	copy(fullKey[len(prefix):], key)

	v, closer, err := s.db.Get(fullKey)
	if err != nil {
		return 0, false
	}

	defer func() { _ = closer.Close() }()

	if len(v) != 8 {
		return 0, false
	}

	return binary.BigEndian.Uint64(v), true
}

// WriteBackfillCursor stores a variable-length cursor ([]byte) for schema rewrite tasks.
func (s *Store) WriteBackfillCursor(batch *dal.WriteSession, key, cursor []byte) error {
	prefix := BackfillKeyPrefix()
	fullKey := make([]byte, len(prefix)+len(key))
	copy(fullKey, prefix)
	copy(fullKey[len(prefix):], key)

	return batch.SetBytes(fullKey, cursor)
}

// ReadBackfillCursor reads a variable-length cursor.
// Returns (cursor, true) if found, (nil, false) if the key does not exist.
func (s *Store) ReadBackfillCursor(key []byte) ([]byte, bool) {
	prefix := BackfillKeyPrefix()
	fullKey := make([]byte, len(prefix)+len(key))
	copy(fullKey, prefix)
	copy(fullKey[len(prefix):], key)

	v, closer, err := s.db.Get(fullKey)
	if err != nil {
		return nil, false
	}

	defer func() { _ = closer.Close() }()

	c := make([]byte, len(v))
	copy(c, v)

	return c, true
}

// DeleteBackfillProgress removes a backfill cursor.
func (s *Store) DeleteBackfillProgress(key []byte) error {
	prefix := BackfillKeyPrefix()
	fullKey := make([]byte, len(prefix)+len(key))
	copy(fullKey, prefix)
	copy(fullKey[len(prefix):], key)

	return s.db.Delete(fullKey, pebble.NoSync)
}

// DeleteBackfillProgressInBatch removes a backfill cursor inside an existing
// batch, so the reset commits atomically with whatever else that batch carries.
// Use it whenever a stale cursor surviving alongside a committed state change
// would be a correctness problem (e.g. a mid-backfill retype that bumps
// pending_version to a fresh, empty keyspace); the direct-DB
// DeleteBackfillProgress remains for standalone task teardown.
func (s *Store) DeleteBackfillProgressInBatch(batch *dal.WriteSession, key []byte) error {
	prefix := BackfillKeyPrefix()
	fullKey := make([]byte, len(prefix)+len(key))
	copy(fullKey, prefix)
	copy(fullKey[len(prefix):], key)

	return batch.DeleteKey(fullKey)
}

// IndexVersionState is the per-replica forward-encoding state for one index.
// Type bindings are meaningful only for metadata indexes. Persisted under
// SubInternalIndexVersion.
type IndexVersionState struct {
	// CurrentVersion is the forward-encoding version actually served
	// by queries on this replica. Zero means the index has never been
	// built locally (no v_n keyspace populated yet).
	CurrentVersion uint32
	// PendingVersion is the target version of an in-flight local
	// rewrite. Zero when no rewrite is running.
	PendingVersion uint32
	// ActivationSequence is the log sequence CurrentVersion's keyspace
	// became complete at. A rewrite stamps every event it writes with the
	// FSM sequence it read from, so a reader pinned below that sequence
	// resolves the promoted keyspace as empty; queries compare their pin
	// against this and refuse rather than serve nothing. Zero for a version
	// built by an initial backfill, whose events carry the sequences of the
	// logs they were folded from and are therefore resolvable at any pin.
	ActivationSequence uint64
	// RewriteProgress is a persisted opaque tail retained by the encoding.
	// The indexbuilder currently stores backfill and schema-rewrite cursors
	// separately under BackfillKey and does not mutate this field.
	RewriteProgress []byte

	// HighWater is the highest forward-encoding version this index has ever
	// allocated on this replica. It is what makes version numbers single-use:
	// dropping the index tombstones the record ({0, 0, HighWater}) instead of
	// deleting it, and a re-created index starts at HighWater+1 — so a fresh
	// builder pass can never write into a keyspace an earlier incarnation
	// already wrote. Events are permanent and stamped with the sequence of
	// the log that caused them; a keyspace shared by two passes can hold two
	// events for one log at one sequence under different encodings, and the
	// retraction such a pass emits loses the same-sequence tie to the
	// standing ADD — an immortal row. Reuse is the only way into that state.
	HighWater uint32

	// CurrentType is the declared metadata type CurrentVersion's rows are
	// encoded under, bound when the version was built and changed only by
	// the atomic switch. It is what makes a retype invisible to queries
	// until the switch: the schema flips at FSM apply, but a query serves
	// CurrentVersion and must validate and encode its conditions under the
	// type those rows actually carry — a condition compiled under the new
	// declared type over old-encoded rows sees only the rows written after
	// the retype (type-tagged encodings occupy disjoint byte ranges), i.e.
	// partial results (EN-1724).
	//
	// CurrentTypeDeclared distinguishes "bound to no declared type" (the
	// key had no schema entry when the version was built; rows carry each
	// value's natural encoding) from METADATA_TYPE_STRING, which is enum
	// value zero. Only meaningful on metadata indexes.
	CurrentType         commonpb.MetadataType
	CurrentTypeDeclared bool

	// PendingType is the declared type PendingVersion's rows are encoded
	// under: the retype's target, bound when the rewrite starts. Live
	// dual-writes encode each value once per version, under that version's
	// bound type.
	PendingType         commonpb.MetadataType
	PendingTypeDeclared bool
}

// Tombstoned reports whether the record marks a dropped index: no servable
// version, no build in flight, only the high-water mark held so the next
// incarnation cannot reuse a version number.
func (s IndexVersionState) Tombstoned() bool {
	return s.CurrentVersion == 0 && s.PendingVersion == 0
}

// IndexVersionStateEntry is the decoded form returned by
// ReadAllIndexVersionStates. CanonicalKey is the [ledger||canonicalID]
// suffix that uniquely identifies the index.
type IndexVersionStateEntry struct {
	LedgerName  string
	CanonicalID string
	State       IndexVersionState
}

// encodeIndexVersionState packs the state to a single byte slice.
// Layout: [current(4B BE)][pending(4B BE)][activation(8B BE)][high_water(4B BE)]
// [current_type(1B)][pending_type(1B)][opaque_tail…].
// A type byte holds 0 for "no declared type bound" and 1+MetadataType
// otherwise, so undeclared stays distinct from METADATA_TYPE_STRING (0).
func encodeIndexVersionState(s IndexVersionState) []byte {
	out := make([]byte, indexVersionStateHeaderLen+len(s.RewriteProgress))
	binary.BigEndian.PutUint32(out[0:4], s.CurrentVersion)
	binary.BigEndian.PutUint32(out[4:8], s.PendingVersion)
	binary.BigEndian.PutUint64(out[8:16], s.ActivationSequence)
	binary.BigEndian.PutUint32(out[16:20], s.HighWater)
	out[20] = encodeBoundType(s.CurrentType, s.CurrentTypeDeclared)
	out[21] = encodeBoundType(s.PendingType, s.PendingTypeDeclared)
	copy(out[indexVersionStateHeaderLen:], s.RewriteProgress)

	return out
}

const indexVersionStateHeaderLen = 22

func encodeBoundType(t commonpb.MetadataType, declared bool) byte {
	if !declared {
		return 0
	}

	return byte(t) + 1
}

func decodeBoundType(b byte) (commonpb.MetadataType, bool) {
	if b == 0 {
		return 0, false
	}

	return commonpb.MetadataType(b - 1), true
}

// decodeIndexVersionState parses a stored value back to IndexVersionState.
// Returns (zero, false) on any malformed input — caller treats it as
// "absent" and re-initializes.
func decodeIndexVersionState(v []byte) (IndexVersionState, bool) {
	if len(v) < indexVersionStateHeaderLen {
		return IndexVersionState{}, false
	}

	progress := make([]byte, len(v)-indexVersionStateHeaderLen)
	copy(progress, v[indexVersionStateHeaderLen:])

	st := IndexVersionState{
		CurrentVersion:     binary.BigEndian.Uint32(v[0:4]),
		PendingVersion:     binary.BigEndian.Uint32(v[4:8]),
		ActivationSequence: binary.BigEndian.Uint64(v[8:16]),
		HighWater:          binary.BigEndian.Uint32(v[16:20]),
		RewriteProgress:    progress,
	}
	st.CurrentType, st.CurrentTypeDeclared = decodeBoundType(v[20])
	st.PendingType, st.PendingTypeDeclared = decodeBoundType(v[21])

	return st, true
}

// WriteIndexVersionState persists the per-replica version state for an
// index. canonicalID must be indexes.Canonical(id) bytes.
func (s *Store) WriteIndexVersionState(batch *dal.WriteSession, ledgerName string, canonicalID string, state IndexVersionState) error {
	key := IndexVersionStateKey(dal.NewKeyBuilder(), ledgerName, canonicalID)

	return batch.SetBytes(key, encodeIndexVersionState(state))
}

// ReadIndexVersionStateFrom reads the per-replica version state for an
// index through the given reader (a snapshot, ReadHandle, or the live
// DB). Returns:
//   - (state, true, nil) when the key exists.
//   - (zero, false, nil) when the key does not exist — equivalent to
//     CurrentVersion=0 with no pending rewrite (i.e. "not yet primed").
//   - (zero, false, err) on a real Pebble I/O failure.
//
// Per CLAUDE.md invariant #7, callers MUST NOT collapse a non-nil err
// into "absent" — a transient I/O error masquerading as `index still
// building` would lie to the client indefinitely.
func ReadIndexVersionStateFrom(reader dal.PebbleGetter, ledgerName, canonicalID string) (IndexVersionState, bool, error) {
	key := IndexVersionStateKey(dal.NewKeyBuilder(), ledgerName, canonicalID)

	v, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return IndexVersionState{}, false, nil
		}

		return IndexVersionState{}, false, fmt.Errorf("reading index version state for %q/%s: %w", ledgerName, canonicalID, err)
	}

	defer func() { _ = closer.Close() }()

	state, ok := decodeIndexVersionState(v)

	return state, ok, nil
}

// ReadIndexVersionState is a convenience wrapper that reads from the
// live DB. Query-path callers should prefer ReadIndexVersionStateFrom
// against the same snapshot/reader they iterate, so the resolved version
// matches the keyspace contents the query will scan (see the "torn read"
// hazard around atomic version switches).
func (s *Store) ReadIndexVersionState(ledgerName, canonicalID string) (IndexVersionState, bool, error) {
	return ReadIndexVersionStateFrom(s.db, ledgerName, canonicalID)
}

// SnapshotVersionResolver returns a closure that resolves per-replica
// index versions via the given reader. The intended call site is right
// after a NewSnapshot() (or ReadHandle creation) so the resolver and
// the iteration share a single point-in-time view — the resolver MUST
// NOT close over the live `*Store` while the caller iterates a
// snapshot, or a concurrent atomic version switch will hand the
// caller a version that does not match the snapshot's keyspace.
//
// Returns (0, error) on a real Pebble I/O failure; (0, nil) when no
// version state has been written yet (caller should translate to
// ErrIndexBuilding at query boundaries).
func SnapshotVersionResolver(reader dal.PebbleGetter, ledgerName string) IndexVersionResolver {
	return PinnedVersionResolver(reader, ledgerName, 0)
}

// ResolvedIndexVersion is what a query learns about an index from the
// version state at its pin: the version to scan and the declared type
// bound to it. The bound type — not the live schema — is what the
// version's rows are encoded under, so conditions must be validated and
// encoded against it (EN-1724); during a retype's conversion window the
// two legitimately differ.
type ResolvedIndexVersion struct {
	Version uint32
	// Type/TypeDeclared mirror IndexVersionState.CurrentType*: the type
	// Version's rows carry, or "none was declared when it was built".
	Type         commonpb.MetadataType
	TypeDeclared bool
	// BindingKnown is true for every resolution built from a stored version
	// state — TypeDeclared=false is then an affirmative "built with no
	// declared type", not missing information. Only query.Compile's
	// pre-versioning test default leaves it false, telling the compiler to
	// fall back to the live schema.
	BindingKnown bool
}

// IndexVersionResolver resolves an index's servable version at the pin of
// the snapshot it was built over. ok=false means no version state exists —
// the index was removed (callers tell it apart from building via the
// registry, see requireIndexReady).
type IndexVersionResolver func(canonical string) (ResolvedIndexVersion, bool, error)

// PinnedVersionResolver is the pin-aware variant: a version promoted by a
// schema rewrite is only servable at pins at or above its activation
// sequence, because the rewrite stamps every event it writes with the one
// FSM sequence it read from. Below that, the promoted keyspace resolves
// empty at the pin — indistinguishable from "no rows match" — so the
// resolver reports the index as not yet live (version 0) and the caller
// surfaces ErrIndexBuilding instead of an empty page.
//
// A pin of 0 means "no pin" (introspection paths that do not resolve rows
// at a sequence) and skips the check.
func PinnedVersionResolver(reader dal.PebbleGetter, ledgerName string, pin uint64) IndexVersionResolver {
	return func(canonical string) (ResolvedIndexVersion, bool, error) {
		state, present, err := ReadIndexVersionStateFrom(reader, ledgerName, canonical)
		if err != nil {
			return ResolvedIndexVersion{}, false, err
		}

		if !present {
			// No record at all. Callers use this to tell a removed index
			// apart from one still being built — see requireIndexReady.
			return ResolvedIndexVersion{}, false, nil
		}

		if state.Tombstoned() {
			// A dropped index. The record survives only to hold the
			// high-water version for the next incarnation; to queries it
			// must read exactly like the removed index it is, never as one
			// still building.
			return ResolvedIndexVersion{}, false, nil
		}

		if pin > 0 && state.ActivationSequence > pin {
			return ResolvedIndexVersion{}, true, nil
		}

		return ResolvedIndexVersion{
			Version:      state.CurrentVersion,
			Type:         state.CurrentType,
			TypeDeclared: state.CurrentTypeDeclared,
			BindingKnown: true,
		}, true, nil
	}
}

// DeleteIndexVersionState removes the per-replica version state for an
// index (e.g. when the index is dropped from the ledger).
func (s *Store) DeleteIndexVersionState(ledgerName string, canonicalID string) error {
	key := IndexVersionStateKey(dal.NewKeyBuilder(), ledgerName, canonicalID)

	return s.db.Delete(key, pebble.NoSync)
}

// ReadAllIndexVersionStates returns every persisted per-index version
// state. Used at boot to rebuild the in-memory map of versions and to
// detect orphan keyspaces for GC.
func (s *Store) ReadAllIndexVersionStates() ([]IndexVersionStateEntry, error) {
	prefix := IndexVersionStatePrefix()
	upper := IncrementBytes(prefix)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating index version state iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var out []IndexVersionStateEntry

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		// Strip the 2-byte prefix [PrefixInternal][SubInternalIndexVersion].
		suffix := k[len(prefix):]
		if len(suffix) < dal.LedgerNameFixedSize+1 {
			continue
		}

		rawName := suffix[:dal.LedgerNameFixedSize]
		end := bytes.IndexByte(rawName, 0)
		if end < 0 {
			end = dal.LedgerNameFixedSize
		}

		ledgerName := string(rawName[:end])
		canonical := string(suffix[dal.LedgerNameFixedSize:])

		v, verr := iter.ValueAndErr()
		if verr != nil {
			return nil, verr
		}

		state, ok := decodeIndexVersionState(v)
		if !ok {
			continue
		}

		out = append(out, IndexVersionStateEntry{
			LedgerName:  ledgerName,
			CanonicalID: canonical,
			State:       state,
		})
	}

	return out, nil
}

// ReadAllBackfillProgress returns all backfill cursors for startup recovery.
func (s *Store) ReadAllBackfillProgress() (map[string]uint64, error) {
	return readAllBackfillProgress(s.db)
}

// ReadAllBackfillProgressFrom is the snapshot-aware variant of
// ReadAllBackfillProgress. Multi-step callers hold a *pebble.Snapshot
// (via NewSnapshot()) and pass it in so every cursor in the returned
// map is coherent with the caller's other snapshot-based reads.
func (s *Store) ReadAllBackfillProgressFrom(reader dal.PebbleReader) (map[string]uint64, error) {
	return readAllBackfillProgress(reader)
}

func readAllBackfillProgress(reader dal.PebbleReader) (map[string]uint64, error) {
	prefix := BackfillKeyPrefix()
	upper := IncrementBytes(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating backfill iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	result := make(map[string]uint64)

	for iter.First(); iter.Valid(); iter.Next() {
		v, verr := iter.ValueAndErr()
		if verr != nil {
			return nil, verr
		}

		if len(v) != 8 {
			continue
		}

		// Strip the prefix bytes from the key for the map key.
		k := iter.Key()
		if len(k) > len(prefix) {
			result[string(k[len(prefix):])] = binary.BigEndian.Uint64(v)
		}
	}

	return result, nil
}

// BackfillEntry is a decoded backfill progress entry returned by ListBackfillProgress.
type BackfillEntry struct {
	LedgerName string
	Kind       byte   // BackfillKindTxBuiltin, etc.
	Details    []byte // kind-specific payload
	Cursor     uint64
}

// ListBackfillProgress reads and decodes all backfill progress entries.
func (s *Store) ListBackfillProgress() ([]BackfillEntry, error) {
	return decodeBackfillProgress(s.ReadAllBackfillProgress())
}

// ListBackfillProgressFrom is the snapshot-aware variant of
// ListBackfillProgress. Multi-step callers reading from a
// *pebble.Snapshot pass it here so the per-cursor values in the
// returned slice come from the same point-in-time view.
func (s *Store) ListBackfillProgressFrom(reader dal.PebbleReader) ([]BackfillEntry, error) {
	return decodeBackfillProgress(s.ReadAllBackfillProgressFrom(reader))
}

func decodeBackfillProgress(all map[string]uint64, err error) ([]BackfillEntry, error) {
	if err != nil {
		return nil, err
	}

	var entries []BackfillEntry

	for key, cursor := range all {
		ledgerName, kind, details, parseErr := ParseBackfillKey([]byte(key))
		if parseErr != nil {
			// A corrupt backfill cursor is corruption, not a legitimate
			// runtime skip (invariant #7) — surface it rather than silently
			// dropping the entry.
			return nil, fmt.Errorf("backfill key %x: %w", key, parseErr)
		}

		entries = append(entries, BackfillEntry{
			LedgerName: ledgerName,
			Kind:       kind,
			Details:    details,
			Cursor:     cursor,
		})
	}

	return entries, nil
}

// checkpointReadyMarker is the sentinel file the index builder writes into a
// query checkpoint read-index directory as the final step, only after the whole
// directory has been atomically renamed into place. Its presence is the single
// authoritative per-replica readiness signal: pebble hard-links SSTs last and a
// checkpoint can fail mid-link (EN-1460's "link ... no such file or directory"),
// so a directory or manifest merely existing is NOT sufficient — a half-written
// or half-linked directory is indistinguishable from a complete one except by
// the marker. The index builder therefore never trusts an unmarked directory; it
// discards and rebuilds from scratch.
const checkpointReadyMarker = ".ready"

// CheckpointDirReady reports whether a query checkpoint read-index directory
// has been fully materialized on THIS replica, i.e. the builder wrote the
// readiness marker as the last step of an atomic materialization.
func CheckpointDirReady(dirPath string) bool {
	_, err := os.Stat(filepath.Join(dirPath, checkpointReadyMarker))

	return err == nil
}

// MarkCheckpointReady writes the readiness marker into a completed checkpoint
// directory and fsyncs both the marker and its parent directory so the marker
// is durable and cannot be observed before the directory content it vouches for.
func MarkCheckpointReady(dirPath string) error {
	markerPath := filepath.Join(dirPath, checkpointReadyMarker)

	f, err := os.OpenFile(markerPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("creating readiness marker: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		return fmt.Errorf("syncing readiness marker: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing readiness marker: %w", err)
	}

	return FsyncDir(dirPath)
}

// FsyncDir fsyncs a directory so a rename/create inside it is durable.
func FsyncDir(dirPath string) error {
	d, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("opening dir for fsync: %w", err)
	}

	if err := d.Sync(); err != nil {
		_ = d.Close()

		return fmt.Errorf("fsync dir: %w", err)
	}

	return d.Close()
}

// WaitForCheckpoint blocks until the query checkpoint read-index directory at
// dirPath is materialized on THIS replica (the .ready marker is present), or the
// context is cancelled. CreateQueryCheckpoint uses it to block on the creator
// node's local marker so the checkpoint is immediately readable there when the
// call returns — replacing the old WaitForSequence-on-cursor fast path, which
// returned before the directory existed (the EN-1460 root cause: the progress
// cursor is persisted in the batch that precedes the physical checkpoint
// creation).
//
// The index builder calls NotifyProgress after each materialization, waking
// waiters to re-check the marker.
func (s *Store) WaitForCheckpoint(ctx context.Context, dirPath string) error {
	if CheckpointDirReady(dirPath) {
		return nil
	}

	// Broadcast on cancellation while holding progressMu. Taking the lock is
	// what closes the missed-wakeup window: the wait loop below holds progressMu
	// across both the ctx.Err() check and cond.Wait(), and Wait() atomically
	// releases the lock only once it is parked — so a cancellation broadcast can
	// never slip between the check and the park.
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			s.progressMu.Lock()
			s.progressCond.Broadcast()
			s.progressMu.Unlock()
		case <-done:
		}
	}()

	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if CheckpointDirReady(dirPath) {
			return nil
		}

		s.progressCond.Wait()
	}
}

// WaitForSequence blocks until the native log-index cursor reaches minSeq or
// the context is cancelled. Query consistency uses WaitForRaftProgress instead;
// this primitive is retained for native-cursor lifecycle checks and tests.
func (s *Store) WaitForSequence(ctx context.Context, minSeq uint64) error {
	// Fast path: already caught up.
	cur, err := s.LastIndexedSequence()
	if err != nil {
		return fmt.Errorf("reading index progress: %w", err)
	}

	if cur >= minSeq {
		return nil
	}

	// Broadcast on cancellation while holding progressMu, exactly as
	// WaitForCheckpoint does. Taking the lock is what closes the missed-wakeup
	// window: the loop below checks ctx.Err() and calls Wait() under the same
	// lock, and Wait releases it only once parked. Broadcasting without the
	// lock can land between that check and Wait, stranding the waiter until an
	// unrelated NotifyProgress arrives — so an alignment wait would outlive
	// its caller's cancellation instead of ending with it.
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			s.progressMu.Lock()
			s.progressCond.Broadcast()
			s.progressMu.Unlock()
		case <-done:
		}
	}()

	s.progressMu.Lock()
	for {
		if ctx.Err() != nil {
			s.progressMu.Unlock()

			return ctx.Err()
		}

		cur, err = s.LastIndexedSequence()
		if err != nil {
			s.progressMu.Unlock()

			return fmt.Errorf("reading index progress: %w", err)
		}

		if cur >= minSeq {
			s.progressMu.Unlock()

			return nil
		}

		s.progressCond.Wait()
	}
}

// WaitForRaftProgress blocks until the normal read projection has certified H.
func (s *Store) WaitForRaftProgress(ctx context.Context, horizon uint64) error {
	return s.waitForProgress(ctx, horizon, s.ReadRaftProgress, "read projection Raft progress")
}

func (s *Store) waitForProgress(ctx context.Context, target uint64, read func() (uint64, error), label string) error {
	cur, err := read()
	if err != nil {
		return fmt.Errorf("reading %s: %w", label, err)
	}
	if cur >= target {
		return nil
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			s.progressMu.Lock()
			s.progressCond.Broadcast()
			s.progressMu.Unlock()
		case <-done:
		}
	}()

	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cur, err = read()
		if err != nil {
			return fmt.Errorf("reading %s: %w", label, err)
		}
		if cur >= target {
			return nil
		}
		s.progressCond.Wait()
	}
}
