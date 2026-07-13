package usagestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/pebblecfg"
)

// DefaultConfig returns the default Pebble configuration for the usage store.
// Reuses the same tunables type as the primary store (pebblecfg.Config).
// Sized smaller than the read index: the usage store holds O(ledgers × templates)
// entries plus a handful of per-ledger counters, so it never grows large.
func DefaultConfig() pebblecfg.Config {
	return pebblecfg.Config{
		MemTableSize:                16 << 20, // 16MB
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       12,
		LBaseMaxBytes:               128 << 20, // 128MB
		CacheSize:                   16 << 20,  // 16MB
		TargetFileSize:              16 << 20,  // 16MB
		BytesPerSync:                512 << 10, // 512KB
		MaxConcurrentCompactions:    1,
		Compression:                 pebblecfg.DefaultLevelCompression(),
	}
}

// Store wraps a Pebble database for the usagebuilder's projections.
// It is a peer to readstore.Store — a distinct physical secondary store,
// so a corruption of one cannot touch the other and each subsystem's
// rebuild story is decoupled (drop the directory + restart).
type Store struct {
	db     *pebble.DB
	logger logging.Logger
	dir    string
}

// New opens or creates a Pebble database at the given directory.
func New(dir string, logger logging.Logger, cfg pebblecfg.Config) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating usage store directory: %w", err)
	}

	dbPath := filepath.Join(dir, "usagedb")

	var fileSize int64
	if info, _ := os.Stat(dbPath); info != nil {
		fileSize = info.Size()
	}

	logger.WithFields(map[string]any{
		"path":     dbPath,
		"fileSize": fileSize,
	}).Infof("Opening Pebble usage store")

	openStart := time.Now()

	cache := pebble.NewCache(cfg.CacheSize)
	defer cache.Unref()

	opts := &pebble.Options{
		Logger:             dal.NewPebbleLogger(logger),
		FormatMajorVersion: pebble.FormatNewest,
		Comparer:           UsageStoreComparer,
		// The usage store is a derived projection rebuilt from the audit log.
		// WAL disabled — on crash the usagebuilder simply replays from its
		// last progress cursor.
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
		return nil, fmt.Errorf("opening Pebble usage store: %w", err)
	}

	m := db.Metrics()
	logger.WithFields(map[string]any{
		"duration":        time.Since(openStart).String(),
		"l0FileCount":     m.Levels[0].TablesCount,
		"l0Size":          m.Levels[0].TablesSize,
		"memTableCount":   m.MemTable.Count,
		"memTableSize":    m.MemTable.Size,
		"totalLevelsSize": m.DiskSpaceUsage(),
	}).Infof("Pebble usage store opened — LSM state")

	return &Store{
		db:     db,
		logger: logger.WithFields(map[string]any{"cmp": "usage-store"}),
		dir:    dir,
	}, nil
}

// OpenReadOnly opens a Pebble usage store at dirPath in read-only mode.
// The caller must call Close() when done.
func OpenReadOnly(dirPath string, logger logging.Logger) (*Store, error) {
	db, err := pebble.Open(dirPath, &pebble.Options{
		Logger:   dal.NewPebbleLogger(logger),
		Comparer: UsageStoreComparer,
		ReadOnly: true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening read-only Pebble usage store at %s: %w", dirPath, err)
	}

	return &Store{
		db:     db,
		logger: logger.WithFields(map[string]any{"cmp": "usage-store-readonly"}),
		dir:    dirPath,
	}, nil
}

// CreateCheckpoint creates a Pebble checkpoint of the usage store at destDir.
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

// NewBatch creates a dal.WriteSession backed by the usage store's Pebble DB.
func (s *Store) NewBatch() *dal.WriteSession {
	return dal.NewWriteSessionFromDB(s.db)
}

// Path returns the directory of the usage store.
func (s *Store) Path() string {
	return s.dir
}

// ReadProgress returns the last log sequence consumed by the usagebuilder.
// Returns 0 if no progress has been recorded.
func (s *Store) ReadProgress() (uint64, error) {
	v, closer, err := s.db.Get(ProgressKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, fmt.Errorf("reading usage progress: %w", err)
	}

	defer func() { _ = closer.Close() }()

	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt usage progress value: expected 8 bytes, got %d", len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// WriteProgress stores the last log sequence consumed by the usagebuilder.
func (s *Store) WriteProgress(batch *dal.WriteSession, sequence uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)

	return batch.SetBytes(ProgressKey(), buf[:])
}

// Reset wipes every projection row (all per-template usage records and all
// per-ledger counters) and clears the persisted progress cursor, so the next
// boot replays from audit sequence 0. Used when the builder detects that the
// primary store was rolled back beneath the persisted cursor: the retained
// rows reflect audit entries that no longer exist, so a clean in-place rebuild
// on the next boot/tick is how the projection reconverges.
//
// The two ledger-scoped prefixes (PrefixTemplate 0x01, PrefixCounter 0x02) are
// contiguous, so one DeleteRange over [0x01, 0x03) covers both; the internal
// progress key ([0xFE][0x01]) is deleted point-wise. A crash mid-reset is
// safe: the cursor is either still ahead (rollback re-detected next boot) or
// already gone (replay from 0), so the rows can never survive with a stale
// non-zero cursor.
func (s *Store) Reset() error {
	batch := s.NewBatch()

	if err := batch.DeleteRangeNoSync([]byte{PrefixTemplate}, []byte{PrefixCounter + 1}); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting projection rows during reset: %w", err)
	}

	if err := batch.DeleteKey(ProgressKey()); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting progress cursor during reset: %w", err)
	}

	if err := batch.Commit(); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("committing usage store reset: %w", err)
	}

	return nil
}

// GetTemplateUsage reads the current usage record for (ledger, template).
// Returns (nil, nil) if no entry exists.
func (s *Store) GetTemplateUsage(ledgerName, templateName string) (*commonpb.TemplateUsage, error) {
	kb := dal.NewKeyBuilder()
	key := TemplateUsageKey(kb, ledgerName, templateName)

	v, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading template usage: %w", err)
	}

	defer func() { _ = closer.Close() }()

	usage := &commonpb.TemplateUsage{}
	if err := usage.UnmarshalVT(v); err != nil {
		return nil, fmt.Errorf("unmarshaling template usage: %w", err)
	}

	return usage, nil
}

// PutTemplateUsage persists a template usage record into the pending batch.
func (s *Store) PutTemplateUsage(batch *dal.WriteSession, ledgerName, templateName string, usage *commonpb.TemplateUsage) error {
	key := TemplateUsageKey(batch.KeyBuilder, ledgerName, templateName)

	return batch.SetProto(key, usage)
}

// GetCounter reads the current value of a per-ledger event counter.
// Returns 0 if no entry exists.
func (s *Store) GetCounter(ledgerName string, counterID byte) (uint64, error) {
	kb := dal.NewKeyBuilder()
	key := CounterKey(kb, ledgerName, counterID)

	v, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, fmt.Errorf("reading counter %#x for ledger %q: %w", counterID, ledgerName, err)
	}

	defer func() { _ = closer.Close() }()

	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt counter value: expected 8 bytes, got %d", len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// PutCounter persists a per-ledger event counter value into the pending batch.
func (s *Store) PutCounter(batch *dal.WriteSession, ledgerName string, counterID byte, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)

	key := CounterKey(batch.KeyBuilder, ledgerName, counterID)

	return batch.SetBytes(key, buf[:])
}
