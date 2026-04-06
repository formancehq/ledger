package readstore

import (
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
)

var progressKey = ProgressKey()

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
		FormatMajorVersion: pebble.FormatNewest,
		// The read index is a derived view rebuilt from the Raft log.
		// We can safely disable WAL: on crash the index builder simply
		// replays from its last progress cursor.
		DisableWAL: true,
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

	logger.WithFields(map[string]any{
		"duration": time.Since(openStart).String(),
		"fileSize": fileSize,
	}).Infof("Pebble read index opened")

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
	db, err := pebble.Open(dirPath, &pebble.Options{ReadOnly: true})
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
	v, closer, err := s.db.Get(progressKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, fmt.Errorf("reading progress: %w", err)
	}

	defer func() { _ = closer.Close() }()

	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt progress value: expected 8 bytes, got %d", len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// WriteProgress stores the last indexed log sequence.
func (s *Store) WriteProgress(batch *pebble.Batch, sequence uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)

	return batch.Set(progressKey, buf[:], pebble.NoSync)
}

// LastIndexedSequence returns the last indexed log sequence (read-only).
func (s *Store) LastIndexedSequence() (uint64, error) {
	return s.ReadProgress()
}

// NotifyProgress wakes all goroutines waiting in WaitForSequence.
// Must be called after WriteProgress commits successfully.
func (s *Store) NotifyProgress() {
	s.progressCond.Broadcast()
}

// WriteBackfillProgress stores a backfill cursor.
func (s *Store) WriteBackfillProgress(batch *pebble.Batch, key []byte, cursor uint64) error {
	fullKey := make([]byte, 1+len(key))
	fullKey[0] = PrefixBackfill
	copy(fullKey[1:], key)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cursor)

	return batch.Set(fullKey, buf[:], pebble.NoSync)
}

// ReadBackfillProgress reads a backfill cursor.
// Returns (cursor, true) if found, (0, false) if the key does not exist.
func (s *Store) ReadBackfillProgress(key []byte) (uint64, bool) {
	fullKey := make([]byte, 1+len(key))
	fullKey[0] = PrefixBackfill
	copy(fullKey[1:], key)

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
func (s *Store) WriteBackfillCursor(batch *pebble.Batch, key, cursor []byte) error {
	fullKey := make([]byte, 1+len(key))
	fullKey[0] = PrefixBackfill
	copy(fullKey[1:], key)

	return batch.Set(fullKey, cursor, pebble.NoSync)
}

// ReadBackfillCursor reads a variable-length cursor.
// Returns (cursor, true) if found, (nil, false) if the key does not exist.
func (s *Store) ReadBackfillCursor(key []byte) ([]byte, bool) {
	fullKey := make([]byte, 1+len(key))
	fullKey[0] = PrefixBackfill
	copy(fullKey[1:], key)

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
	fullKey := make([]byte, 1+len(key))
	fullKey[0] = PrefixBackfill
	copy(fullKey[1:], key)

	return s.db.Delete(fullKey, pebble.NoSync)
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

		// Strip the prefix byte from the key for the map key.
		k := iter.Key()
		if len(k) > 1 {
			result[string(k[1:])] = binary.BigEndian.Uint64(v)
		}
	}

	return result, nil
}

// SchemaRewriteEntry is a decoded schema rewrite progress entry.
type SchemaRewriteEntry struct {
	Ledger     string
	TargetType byte   // from key: [ledger\x00]S[targetType_byte][key]
	Key        string // metadata field name
	BBKey      []byte // key without the prefix byte (for backfill operations)
	ToType     byte   // first byte of value
	Cursor     []byte // remaining bytes of value (reverse map cursor)
}

// ReadAllSchemaRewriteProgress reads all schema rewrite entries from the backfill prefix.
func (s *Store) ReadAllSchemaRewriteProgress() ([]SchemaRewriteEntry, error) {
	prefix := BackfillKeyPrefix()
	upper := IncrementBytes(prefix)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating schema rewrite iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var entries []SchemaRewriteEntry

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) < 2 {
			continue
		}

		// Strip prefix byte.
		innerKey := k[1:]
		ledger, kind, details, ok := ParseBackfillKey(innerKey)

		if !ok || kind != BackfillKindSchemaRewrite {
			continue
		}

		if len(details) < 1 {
			continue
		}

		v, verr := iter.ValueAndErr()
		if verr != nil {
			return nil, verr
		}

		targetType := details[0]
		metaKey := string(details[1:])

		var toType byte
		var cursor []byte

		if len(v) >= 1 {
			toType = v[0]
			if len(v) > 1 {
				cursor = make([]byte, len(v)-1)
				copy(cursor, v[1:])
			}
		}

		bbKey := make([]byte, len(innerKey))
		copy(bbKey, innerKey)

		entries = append(entries, SchemaRewriteEntry{
			Ledger:     ledger,
			TargetType: targetType,
			Key:        metaKey,
			BBKey:      bbKey,
			ToType:     toType,
			Cursor:     cursor,
		})
	}

	return entries, nil
}

// BackfillEntry is a decoded backfill progress entry returned by ListBackfillProgress.
type BackfillEntry struct {
	Ledger  string
	Kind    byte   // BackfillKindTxBuiltin, etc.
	Details []byte // kind-specific payload
	Cursor  uint64
}

// ListBackfillProgress reads and decodes all backfill progress entries.
func (s *Store) ListBackfillProgress() ([]BackfillEntry, error) {
	all, err := s.ReadAllBackfillProgress()
	if err != nil {
		return nil, err
	}

	var entries []BackfillEntry

	for key, cursor := range all {
		ledger, kind, details, ok := ParseBackfillKey([]byte(key))
		if !ok {
			continue
		}

		entries = append(entries, BackfillEntry{
			Ledger:  ledger,
			Kind:    kind,
			Details: details,
			Cursor:  cursor,
		})
	}

	return entries, nil
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
