package readstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	bolt "go.etcd.io/bbolt"
)

var progressKey = []byte("lastSeq")

// Store wraps a bbolt database for the read-side inverted indexes.
// It is safe for concurrent use: bbolt supports one writer and many
// concurrent readers via MVCC snapshots.
type Store struct {
	db     *bolt.DB
	logger logging.Logger
	path   string
}

// New opens or creates a bbolt database at the given directory.
// It creates all required buckets on first open.
func New(dir string, logger logging.Logger) (*Store, error) {
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
		"path":     dbPath,
		"fileSize": fileSize,
	}).Infof("Opening bbolt read index")
	openStart := time.Now()
	db, err := bolt.Open(dbPath, 0o600, nil)
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
			BucketReverseMap,
			BucketAccountTx,
			BucketSourceAccountTx,
			BucketDestAccountTx,
			BucketProgress,
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

	return &Store{
		db:     db,
		logger: logger.WithFields(map[string]any{"cmp": "read-store"}),
		path:   dbPath,
	}, nil
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
		return fmt.Errorf("progress bucket not found")
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
