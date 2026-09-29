package usagestore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/linxGnu/grocksdb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/rocksdbcfg"
)

// DefaultConfig returns the default RocksDB configuration for the usage store.
// Reuses the same tunables type as the primary store (rocksdbcfg.Config).
// Sized smaller than the read index: the usage store holds O(ledgers × templates)
// entries plus a handful of per-ledger counters, so it never grows large.
func DefaultConfig() rocksdbcfg.Config {
	return rocksdbcfg.Config{
		MemTableSize:                16 << 20, // 16MB
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       12,
		LBaseMaxBytes:               128 << 20, // 128MB
		CacheSize:                   16 << 20,  // 16MB
		TargetFileSize:              16 << 20,  // 16MB
		BytesPerSync:                512 << 10, // 512KB
		MaxConcurrentCompactions:    1,
		Compression:                 rocksdbcfg.DefaultLevelCompression(),
	}
}

// Store wraps a RocksDB database for the usagebuilder's projections.
// It is a peer to readstore.Store — a distinct physical secondary store,
// so a corruption of one cannot touch the other and each subsystem's
// rebuild story is decoupled (drop the directory + restart).
type Store struct {
	db           *grocksdb.DB
	options      *grocksdb.Options
	cache        *grocksdb.Cache
	tableOptions *grocksdb.BlockBasedTableOptions
	writeOptions *grocksdb.WriteOptions
	logger       logging.Logger
	dir          string
	readOnly     bool
}

// New opens or creates a RocksDB database at the given directory.
func New(dir string, logger logging.Logger, cfg rocksdbcfg.Config) (*Store, error) {
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
	}).Infof("Opening RocksDB usage store")

	openStart := time.Now()

	opts, cache, table, err := usageOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("configuring RocksDB usage store: %w", err)
	}
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		table.Destroy()
		cache.Destroy()
		opts.Destroy()

		return nil, fmt.Errorf("opening RocksDB usage store: %w", err)
	}
	writeOptions := grocksdb.NewDefaultWriteOptions()
	writeOptions.DisableWAL(true)
	logger.WithFields(map[string]any{"duration": time.Since(openStart).String()}).Infof("RocksDB usage store opened")

	return &Store{
		db:      db,
		options: opts, cache: cache, tableOptions: table, writeOptions: writeOptions,
		logger: logger.WithFields(map[string]any{"cmp": "usage-store"}),
		dir:    dir,
	}, nil
}

// OpenReadOnly opens a RocksDB usage store at dirPath in read-only mode.
// The caller must call Close() when done.
func OpenReadOnly(dirPath string, logger logging.Logger) (*Store, error) {
	opts, cache, table, err := usageOptions(DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("configuring read-only RocksDB usage store: %w", err)
	}
	db, err := grocksdb.OpenDbForReadOnly(opts, dirPath, false)
	if err != nil {
		table.Destroy()
		cache.Destroy()
		opts.Destroy()

		return nil, fmt.Errorf("opening read-only RocksDB usage store at %s: %w", dirPath, err)
	}

	return &Store{db: db, options: opts, cache: cache, tableOptions: table, logger: logger.WithFields(map[string]any{"cmp": "usage-store-readonly"}), dir: dirPath, readOnly: true}, nil
}

func usageOptions(cfg rocksdbcfg.Config) (*grocksdb.Options, *grocksdb.Cache, *grocksdb.BlockBasedTableOptions, error) {
	// grocksdb.Options.Destroy double-frees a SliceTransform passed through
	// SetPrefixExtractor with RocksDB 11.8. Parsing the native capped transform
	// leaves grocksdb's duplicate cst ownership slot nil while retaining the
	// extractor in RocksDB's options and SST metadata.
	opts, err := grocksdb.GetOptionsFromString(nil, fmt.Sprintf("prefix_extractor=capped:%d", ledgerScopedPrefixLen))
	if err != nil {
		return nil, nil, nil, err
	}
	opts.SetCreateIfMissing(true)
	cmp := newUsageStoreComparator()
	opts.SetComparator(cmp)
	opts.SetWriteBufferSize(cfg.MemTableSize)
	opts.SetMaxWriteBufferNumber(cfg.MemTableStopWritesThreshold)
	opts.SetLevel0FileNumCompactionTrigger(cfg.L0CompactionThreshold)
	opts.SetLevel0StopWritesTrigger(cfg.L0StopWritesThreshold)
	opts.SetMaxBytesForLevelBase(uint64(cfg.LBaseMaxBytes))
	opts.SetTargetFileSizeBase(uint64(cfg.TargetFileSize))
	opts.SetCompressionPerLevel(cfg.RocksDBCompression())
	opts.SetBytesPerSync(uint64(cfg.BytesPerSync))
	opts.SetMaxBackgroundJobs(cfg.MaxConcurrentCompactions + 1)
	cache := grocksdb.NewLRUCache(uint64(cfg.CacheSize))
	table := grocksdb.NewDefaultBlockBasedTableOptions()
	table.SetBlockCache(cache)
	table.SetFilterPolicy(grocksdb.NewBloomFilterFull(10))
	table.SetWholeKeyFiltering(false)
	opts.SetBlockBasedTableFactory(table)

	return opts, cache, table, nil
}

// CreateCheckpoint includes WAL-less committed rows by flushing first.
func (s *Store) CreateCheckpoint(destDir string) error {
	if _, err := os.Stat(destDir); err == nil {
		return fmt.Errorf("checkpoint destination already exists: %s", destDir)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := s.Flush(); err != nil {
		return err
	}
	cp, err := s.db.NewCheckpoint()
	if err != nil {
		return err
	}
	defer cp.Destroy()

	return cp.CreateCheckpoint(destDir, 0)
}

func (s *Store) Flush() error {
	opts := grocksdb.NewDefaultFlushOptions()
	defer opts.Destroy()
	opts.SetWait(true)

	return s.db.Flush(opts)
}

// Close flushes the WAL-less projection before releasing native resources.
func (s *Store) Close() error {
	var flushErr error
	if !s.readOnly {
		flushErr = s.Flush()
	}
	s.db.Close()
	if s.writeOptions != nil {
		s.writeOptions.Destroy()
	}
	s.tableOptions.Destroy()
	s.cache.Destroy()
	s.options.Destroy()

	return flushErr
}

func (s *Store) DB() *grocksdb.DB { return s.db }
func (s *Store) NewBatch() *WriteSession {
	return &WriteSession{db: s.db, options: s.writeOptions, batch: grocksdb.NewWriteBatch(), KeyBuilder: dal.NewKeyBuilder()}
}
func (s *Store) Path() string { return s.dir }

func (s *Store) get(key []byte) ([]byte, error) {
	opts := grocksdb.NewDefaultReadOptions()
	defer opts.Destroy()
	slice, err := s.db.Get(opts, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return nil, nil
	}

	return append([]byte{}, slice.Data()...), nil
}

// ReadProgress returns the last audit sequence consumed by the usagebuilder.
// Returns 0 if no progress has been recorded.
func (s *Store) ReadProgress() (uint64, error) {
	v, err := s.get(ProgressKey())
	if err != nil {
		return 0, fmt.Errorf("reading usage progress: %w", err)
	}
	if v == nil {
		return 0, nil
	}

	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt usage progress value: expected 8 bytes, got %d", len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// WriteProgress stores the last log sequence consumed by the usagebuilder.
func (s *Store) WriteProgress(batch *WriteSession, sequence uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)

	return batch.SetBytes(ProgressKey(), buf[:])
}

// Reset wipes every projection row (all per-template usage records and all
// per-ledger counters) and clears the persisted progress cursor, so the next
// boot replays from audit sequence 0. It is a full-rebuild primitive used when
// the primary store was restored below the usage cursor. The permanent audit
// history is the complete reconstruction source.
//
// The two ledger-scoped prefixes (PrefixTemplate 0x01, PrefixCounter 0x02) are
// contiguous, so one DeleteRange over [0x01, 0x03) covers both; the internal
// progress key ([0xFE][0x01]) is deleted point-wise. Rows and cursor are wiped
// in a single RocksDB batch and flushed before return. The batch commit makes
// the reset atomically visible; the flush makes that atomic state durable
// before the builder publishes cursor zero or begins replaying.
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

	if err := s.Flush(); err != nil {
		return fmt.Errorf("flushing usage store reset: %w", err)
	}

	return nil
}

// GetTemplateUsage reads the current usage record for (ledger, template).
// Returns (nil, nil) if no entry exists.
func (s *Store) GetTemplateUsage(ledgerName, templateName string) (*commonpb.TemplateUsage, error) {
	kb := dal.NewKeyBuilder()
	key := TemplateUsageKey(kb, ledgerName, templateName)

	v, err := s.get(key)
	if err != nil {
		return nil, fmt.Errorf("reading template usage: %w", err)
	}
	if v == nil {
		return nil, nil
	}

	usage := &commonpb.TemplateUsage{}
	if err := usage.UnmarshalVT(v); err != nil {
		return nil, fmt.Errorf("unmarshaling template usage: %w", err)
	}

	return usage, nil
}

// PutTemplateUsage persists a template usage record into the pending batch.
func (s *Store) PutTemplateUsage(batch *WriteSession, ledgerName, templateName string, usage *commonpb.TemplateUsage) error {
	key := TemplateUsageKey(batch.KeyBuilder, ledgerName, templateName)

	return batch.SetProto(key, usage)
}

// GetCounter reads the current value of a per-ledger event counter.
// Returns 0 if no entry exists.
func (s *Store) GetCounter(ledgerName string, counterID byte) (uint64, error) {
	kb := dal.NewKeyBuilder()
	key := CounterKey(kb, ledgerName, counterID)

	v, err := s.get(key)
	if err != nil {
		return 0, fmt.Errorf("reading counter %#x for ledger %q: %w", counterID, ledgerName, err)
	}
	if v == nil {
		return 0, nil
	}

	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt counter value: expected 8 bytes, got %d", len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// PutCounter persists a per-ledger event counter value into the pending batch.
func (s *Store) PutCounter(batch *WriteSession, ledgerName string, counterID byte, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)

	key := CounterKey(batch.KeyBuilder, ledgerName, counterID)

	return batch.SetBytes(key, buf[:])
}
