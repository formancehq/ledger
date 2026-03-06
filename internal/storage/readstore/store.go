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

	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/go-libs/v3/logging"
)

var progressKey = []byte("lastSeq")

// Store wraps a bbolt database for the read-side inverted indexes.
// It is safe for concurrent use: bbolt supports one writer and many
// concurrent readers via MVCC snapshots.
type Store struct {
	db     *bolt.DB
	logger logging.Logger
	path   string

	// Options used to open the database, needed to reopen after compaction.
	noFreelistSync  bool
	initialMmapSize int

	// progressMu and progressCond allow callers to wait until the indexed
	// sequence reaches a target value.  The index builder calls
	// NotifyProgress after each WriteProgress to wake up waiters.
	progressMu   sync.Mutex
	progressCond *sync.Cond
}

// DefaultInitialMmapSize is the default initial mmap size for the bbolt database (1 GiB).
// Pre-allocating virtual address space prevents mmap stalls as the DB grows.
const DefaultInitialMmapSize = 1 << 30

// New opens or creates a bbolt database at the given directory.
// It creates all required buckets on first open.
// When noFreelistSync is true, bbolt skips serializing the freelist on each
// commit, which significantly reduces CPU during bulk writes. The freelist
// is rebuilt from a full page scan on the next Open().
// When initialMmapSize is 0, DefaultInitialMmapSize (1 GiB) is used.
func New(dir string, noFreelistSync bool, initialMmapSize int, logger logging.Logger) (*Store, error) {
	if initialMmapSize == 0 {
		initialMmapSize = DefaultInitialMmapSize
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating read store directory: %w", err)
	}

	dbPath := filepath.Join(dir, "readindex.db")
	info, _ := os.Stat(dbPath)

	var fileSize int64
	if info != nil {
		fileSize = info.Size()
	}

	logger.WithFields(map[string]any{
		"path":            dbPath,
		"fileSize":        fileSize,
		"noFreelistSync":  noFreelistSync,
		"initialMmapSize": initialMmapSize,
	}).Infof("Opening bbolt read index")

	openStart := time.Now()

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{
		// The read index is a derived view rebuilt from Pebble (the Raft log
		// is the source of truth). We can safely disable fsync: on crash the
		// index builder simply replays from its last progress cursor.
		NoSync: true,
		// O(1) page alloc/dealloc instead of O(n) with the default array type.
		FreelistType: bolt.FreelistMapType,
		// When enabled, skip writing the freelist to disk on each commit.
		// The freelist is rebuilt by scanning all pages on the next Open().
		// This trades slower startup for faster bulk-write throughput.
		NoFreelistSync: noFreelistSync,
		// Pre-allocate virtual address space so mmap doesn't need to
		// grow (and stall the writer) as the database file expands.
		InitialMmapSize: initialMmapSize,
	})
	if err != nil {
		return nil, fmt.Errorf("opening bbolt database: %w", err)
	}

	logger.WithFields(map[string]any{
		"duration": time.Since(openStart).String(),
		"fileSize": fileSize,
	}).Infof("bbolt read index opened")

	// Create all required buckets.
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{
			BucketMetadataIndex,
			BucketExistence,
			BucketEntityExists,
			BucketReverseMap,
			BucketAccountTx,
			BucketSourceAccountTx,
			BucketDestAccountTx,
			BucketProgress,
			BucketBackfill,
			BucketTransactionReference,
			BucketTransactionTimestamp,
			BucketLedgerLogs,
		} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return fmt.Errorf("creating bucket %q: %w", string(bucket), err)
			}
		}

		return nil
	}); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("initializing buckets: %w", err)
	}

	s := &Store{
		db:              db,
		logger:          logger.WithFields(map[string]any{"cmp": "read-store"}),
		path:            dbPath,
		noFreelistSync:  noFreelistSync,
		initialMmapSize: initialMmapSize,
	}
	s.progressCond = sync.NewCond(&s.progressMu)

	return s, nil
}

// SyncFreelist persists the in-memory freelist to disk so that the next
// Open() can load it directly instead of scanning all pages (O(N) on the
// number of database pages). This is useful after bulk writes with
// NoFreelistSync=true: call SyncFreelist once before Close to avoid a
// very slow page scan on the next startup.
//
// The method temporarily disables NoFreelistSync and performs a no-op
// read-write transaction whose Commit() writes the freelist page.
func (s *Store) SyncFreelist() error {
	s.logger.Infof("Syncing bbolt freelist to disk (may take a moment for large databases)...")

	start := time.Now()

	// Temporarily enable freelist serialization for this single commit,
	// then restore NoFreelistSync. bbolt serializes all writers so there
	// is no concurrent access to this field while we hold the write lock.
	s.db.NoFreelistSync = false
	err := s.db.Update(func(_ *bolt.Tx) error { return nil })
	s.db.NoFreelistSync = true

	if err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to sync freelist")

		return fmt.Errorf("syncing freelist: %w", err)
	}

	s.logger.WithFields(map[string]any{"duration": time.Since(start).String()}).Infof("Freelist synced to disk")

	return nil
}

// RunPeriodicFreelistSync syncs the freelist to disk at the given interval.
// This ensures that after a crash, the next Open() can load the freelist
// directly instead of scanning all pages (which can take tens of minutes
// on large databases). The goroutine exits when the context is cancelled.
func (s *Store) RunPeriodicFreelistSync(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.logger.WithFields(map[string]any{
		"interval": interval.String(),
	}).Infof("Starting periodic freelist sync")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := s.SyncFreelist()
			if err != nil {
				s.logger.WithFields(map[string]any{
					"error": err.Error(),
				}).Errorf("Periodic freelist sync failed")
			}
		}
	}
}

// Close closes the underlying bbolt database.
func (s *Store) Close() error {
	return s.db.Close()
}

// View opens a read-only transaction. All cursors opened within the callback
// see the same consistent snapshot (bbolt MVCC).
func (s *Store) View(fn func(tx *bolt.Tx) error) error {
	return s.db.View(fn)
}

// Update opens a read-write transaction. Only one Update can run at a time.
func (s *Store) Update(fn func(tx *bolt.Tx) error) error {
	return s.db.Update(fn)
}

// DB returns the underlying bbolt database for advanced use cases.
func (s *Store) DB() *bolt.DB {
	return s.db
}

// Path returns the file path of the bbolt database.
func (s *Store) Path() string {
	return s.path
}

// ReadProgress returns the last indexed log sequence from the progress bucket.
// Returns 0 if no progress has been recorded.
func (s *Store) ReadProgress(tx *bolt.Tx) (uint64, error) {
	b := tx.Bucket(BucketProgress)
	if b == nil {
		return 0, nil
	}

	v := b.Get(progressKey)
	if v == nil {
		return 0, nil
	}

	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt progress value: expected 8 bytes, got %d", len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// WriteProgress stores the last indexed log sequence in the progress bucket.
func (s *Store) WriteProgress(tx *bolt.Tx, sequence uint64) error {
	b := tx.Bucket(BucketProgress)
	if b == nil {
		return errors.New("progress bucket not found")
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)

	return b.Put(progressKey, buf[:])
}

// LastIndexedSequence returns the last indexed log sequence (read-only).
func (s *Store) LastIndexedSequence() (uint64, error) {
	var seq uint64

	err := s.db.View(func(tx *bolt.Tx) error {
		var readErr error

		seq, readErr = s.ReadProgress(tx)

		return readErr
	})

	return seq, err
}

// NotifyProgress wakes all goroutines waiting in WaitForSequence.
// Must be called after WriteProgress commits successfully.
func (s *Store) NotifyProgress() {
	s.progressCond.Broadcast()
}

// WriteBackfillProgress stores a backfill cursor in the backfill bucket.
func (s *Store) WriteBackfillProgress(tx *bolt.Tx, key []byte, cursor uint64) error {
	b := tx.Bucket(BucketBackfill)
	if b == nil {
		return errors.New("backfill bucket not found")
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cursor)

	return b.Put(key, buf[:])
}

// ReadBackfillProgress reads a backfill cursor from the backfill bucket.
// Returns (cursor, true) if found, (0, false) if the key does not exist.
func (s *Store) ReadBackfillProgress(tx *bolt.Tx, key []byte) (uint64, bool) {
	b := tx.Bucket(BucketBackfill)
	if b == nil {
		return 0, false
	}

	v := b.Get(key)
	if v == nil || len(v) != 8 {
		return 0, false
	}

	return binary.BigEndian.Uint64(v), true
}

// DeleteBackfillProgress removes a backfill cursor from the backfill bucket.
func (s *Store) DeleteBackfillProgress(tx *bolt.Tx, key []byte) error {
	b := tx.Bucket(BucketBackfill)
	if b == nil {
		return nil
	}

	return b.Delete(key)
}

// ReadAllBackfillProgress returns all backfill cursors for startup recovery.
func (s *Store) ReadAllBackfillProgress(tx *bolt.Tx) (map[string]uint64, error) {
	b := tx.Bucket(BucketBackfill)
	if b == nil {
		return nil, nil
	}

	result := make(map[string]uint64)

	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(v) != 8 {
			continue
		}

		result[string(k)] = binary.BigEndian.Uint64(v)
	}

	return result, nil
}

// BackfillEntry is a decoded backfill progress entry returned by ListBackfillProgress.
type BackfillEntry struct {
	Ledger  string
	Kind    byte   // BackfillKindTxBuiltin, BackfillKindTxMetadata, BackfillKindAcctBuiltin, BackfillKindAcctMetadata, or BackfillKindLogBuiltin
	Details []byte // kind-specific payload: builtin byte, or metadata key string
	Cursor  uint64
}

// ListBackfillProgress reads and decodes all backfill progress entries from bbolt.
// Entries with unrecognised key formats are silently skipped.
func (s *Store) ListBackfillProgress() ([]BackfillEntry, error) {
	var entries []BackfillEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		all, err := s.ReadAllBackfillProgress(tx)
		if err != nil {
			return err
		}

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

		return nil
	})

	return entries, err
}

// WaitForSequence blocks until LastIndexedSequence >= minSeq or the context
// is cancelled.  Returns nil when the target is reached, or ctx.Err() on
// cancellation / timeout.
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
