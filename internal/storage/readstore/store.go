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
	progressMu   sync.Mutex
	progressCond *sync.Cond
}

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
		db:     db,
		logger: logger.WithFields(map[string]any{"cmp": "read-store-readonly"}),
		dir:    dirPath,
	}
	s.progressCond = sync.NewCond(&s.progressMu)

	return s, nil
}

// CreateCheckpoint creates a Pebble checkpoint of the read index at destDir.
// Since the read index has WAL disabled, no WAL flush option is needed.
func (s *Store) CreateCheckpoint(destDir string) error {
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

// WriteProgress stores the last indexed log sequence.
func (s *Store) WriteProgress(batch *dal.WriteSession, sequence uint64) error {
	return progressCursor.Write(batch, sequence)
}

// LastIndexedSequence returns the last indexed log sequence (read-only).
func (s *Store) LastIndexedSequence() (uint64, error) {
	return s.ReadProgress()
}

// NotifyProgress wakes all goroutines waiting in WaitForSequence /
// WaitForCheckpoint. Must be called after WriteProgress commits successfully.
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

// IndexVersionState is the per-replica forward-encoding state for a
// single metadata index. Persisted under SubInternalIndexVersion.
type IndexVersionState struct {
	// CurrentVersion is the forward-encoding version actually served
	// by queries on this replica. Zero means the index has never been
	// built locally (no v_n keyspace populated yet).
	CurrentVersion uint32
	// PendingVersion is the target version of an in-flight local
	// rewrite. Zero when no rewrite is running.
	PendingVersion uint32
	// RewriteProgress is the cursor of the in-flight rewrite (e.g. the
	// last reverse-map key processed). Empty when no rewrite is
	// running. Variable-length, opaque to the readstore.
	RewriteProgress []byte
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
// Layout: [current(4B BE)][pending(4B BE)][rewrite_progress…].
func encodeIndexVersionState(s IndexVersionState) []byte {
	out := make([]byte, 8+len(s.RewriteProgress))
	binary.BigEndian.PutUint32(out[0:4], s.CurrentVersion)
	binary.BigEndian.PutUint32(out[4:8], s.PendingVersion)
	copy(out[8:], s.RewriteProgress)

	return out
}

// decodeIndexVersionState parses a stored value back to IndexVersionState.
// Returns (zero, false) on any malformed input — caller treats it as
// "absent" and re-initializes.
func decodeIndexVersionState(v []byte) (IndexVersionState, bool) {
	if len(v) < 8 {
		return IndexVersionState{}, false
	}

	progress := make([]byte, len(v)-8)
	copy(progress, v[8:])

	return IndexVersionState{
		CurrentVersion:  binary.BigEndian.Uint32(v[0:4]),
		PendingVersion:  binary.BigEndian.Uint32(v[4:8]),
		RewriteProgress: progress,
	}, true
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
func SnapshotVersionResolver(reader dal.PebbleGetter, ledgerName string) func(canonical string) (uint32, error) {
	return func(canonical string) (uint32, error) {
		state, _, err := ReadIndexVersionStateFrom(reader, ledgerName, canonical)
		if err != nil {
			return 0, err
		}

		return state.CurrentVersion, nil
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
	prefix := BackfillKeyPrefix()
	upper := IncrementBytes(prefix)

	iter, err := s.db.NewIter(&pebble.IterOptions{
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
	all, err := s.ReadAllBackfillProgress()
	if err != nil {
		return nil, err
	}

	var entries []BackfillEntry

	for key, cursor := range all {
		ledgerName, kind, details, ok := ParseBackfillKey([]byte(key))
		if !ok {
			continue
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

// WaitForSequence blocks until LastIndexedSequence >= minSeq or the context
// is cancelled.
func (s *Store) WaitForSequence(ctx context.Context, minSeq uint64) error {
	// Fast path: already caught up.
	cur, err := s.LastIndexedSequence()
	if err != nil {
		return fmt.Errorf("reading index progress: %w", err)
	}

	if cur >= minSeq {
		return nil
	}

	// Spawn a goroutine that broadcasts when the context is cancelled so
	// the Wait() below is unblocked.
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			s.progressCond.Broadcast()
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
