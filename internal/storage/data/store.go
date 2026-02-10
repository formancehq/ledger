package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

const (
	liveDir               = "live"
	currentCheckpointFile = "CURRENT_CHECKPOINT"
	checkpointsDir        = "checkpoints"
)

// Store is a Pebble implementation of data.Store
// It stores balances and account metadata
type Store struct {
	db                *pebble.DB
	opts              *pebble.Options
	logger            logging.Logger
	dataDir           string
	currentCheckPoint uint64
	oldestCheckpoint  uint64
	maxCheckpoints    int
}

// Key prefixes for Pebble storage
var (
	keyPrefixLastAppliedIndex  byte = 0x00 // [keyPrefixLastAppliedIndex] -> uint64
	keyPrefixLog               byte = 0x01 // [keyPrefixLog][sequence] -> Log
	keyPrefixIdempotency       byte = 0x02 // [keyPrefixIdempotency][key] -> sequence
	keyPrefixLedgerInfo        byte = 0x03 // [keyPrefixLedgerInfo][ledgerID] -> LedgerInfo
	keyPrefixTransactionUpdate byte = 0x08 // [ledger][keyPrefixTransactionUpdate][transactionID][byLog] -> TransactionUpdate
	KeyPrefixAttributes        byte = 0x09

	AttributePrefixInput          = byte('I')
	AttributePrefixOutput         = byte('O')
	AttributePrefixMetadata       = byte('M')
	AttributePrefixLedgerMetadata = byte('L')
	AttributePrefixReverted       = byte('R')
	AttributePrefixIdempotencyKey = byte('K')
	AttributePrefixLedger         = byte('G')
	AttributePrefixBoundary       = byte('B')
)

// NewStore creates a new Store instance
func NewStore(
	dataDir string,
	logger logging.Logger,
	meter metric.Meter,
	cfg Config,
) (*Store, error) {

	opts := &pebble.Options{
		EventListener: NewMetricsListener(meter),
		// 1) Absorb more writes before flush => fewer SST files, fewer compactions.
		MemTableSize:                cfg.MemTableSize,
		MemTableStopWritesThreshold: cfg.MemTableStopWritesThreshold,

		// 2) Control L0 pressure (main source of compactions/churn in write-heavy workloads).
		L0CompactionThreshold: cfg.L0CompactionThreshold,
		L0StopWritesThreshold: cfg.L0StopWritesThreshold,
		LBaseMaxBytes:         cfg.LBaseMaxBytes,
		Cache:                 pebble.NewCache(cfg.CacheSize),

		// 3) Table sizes: fewer small files => fewer compactions.
		Levels: []pebble.LevelOptions{
			{TargetFileSize: cfg.TargetFileSize},
		},

		// 4) Smooth IO during flush/compactions.
		BytesPerSync:    cfg.BytesPerSync,
		WALBytesPerSync: cfg.WALBytesPerSync,

		// 5) Compaction concurrency: OK but not too high (otherwise you saturate IO).
		MaxConcurrentCompactions: func() int { return cfg.MaxConcurrentCompactions },

		// 6) WAL configuration
		WALMinSyncInterval: func() time.Duration { return cfg.WALMinSyncInterval },
		DisableWAL:         cfg.DisableWAL,
	}

	var (
		db *pebble.DB
	)

	currentCheckpointRaw, err := os.ReadFile(filepath.Join(dataDir, currentCheckpointFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading current checkpoint: %w", err)
	}
	liveDir := filepath.Join(dataDir, liveDir)
	if errors.Is(err, os.ErrNotExist) {
		logger.Infof("No checkpoint found, creating new database in %s", liveDir)
		if err := os.RemoveAll(liveDir); err != nil {
			return nil, fmt.Errorf("removing old database: %w", err)
		}

		db, err = pebble.Open(liveDir, opts)
		if err != nil {
			return nil, fmt.Errorf("opening pebble database: %w", err)
		}

		if err := db.Checkpoint(filepath.Join(dataDir, checkpointsDir, "0")); err != nil {
			return nil, fmt.Errorf("creating initial checkpoint: %w", err)
		}

		f, err := os.Create(filepath.Join(dataDir, currentCheckpointFile))
		if err != nil {
			return nil, fmt.Errorf("creating current checkpoint file: %w", err)
		}

		if _, err := f.WriteString("0"); err != nil {
			return nil, fmt.Errorf("writing current checkpoint: %w", err)
		}

		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("syncing current checkpoint file: %w", err)
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("closing current checkpoint file: %w", err)
		}
	} else {
		logger.Infof("Checkpoint found, restoring from checkpoint %s to directory %s", string(currentCheckpointRaw), liveDir)
		if err := os.RemoveAll(liveDir); err != nil {
			return nil, fmt.Errorf("removing old database: %w", err)
		}

		if err := HardLink(filepath.Join(dataDir, checkpointsDir, string(currentCheckpointRaw)), liveDir); err != nil {
			return nil, fmt.Errorf("hard linking checkpoint: %w", err)
		}

		db, err = pebble.Open(liveDir, opts)
		if err != nil {
			return nil, fmt.Errorf("opening pebble database: %w", err)
		}
	}

	var currentCheckpoint uint64
	if len(currentCheckpointRaw) > 0 {
		currentCheckpoint, err = strconv.ParseUint(string(currentCheckpointRaw), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	// Calculate the oldest checkpoint that should exist
	// based on current checkpoint and max checkpoints configuration
	var oldestCheckpoint uint64
	if currentCheckpoint >= uint64(cfg.MaxCheckpoints) {
		oldestCheckpoint = currentCheckpoint - uint64(cfg.MaxCheckpoints) + 1
	}

	return &Store{
		db:                db,
		opts:              opts,
		logger:            logger.WithField("cmp", "pebble"),
		dataDir:           dataDir,
		currentCheckPoint: currentCheckpoint,
		oldestCheckpoint:  oldestCheckpoint,
		maxCheckpoints:    cfg.MaxCheckpoints,
	}, nil
}

// Close closes the Pebble database
func (s *Store) Close() error {
	var errs []error
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing store: %v", errs)
	}
	return nil
}

// GetLogBySequence retrieves a log by its sequence number.
func (s *Store) GetLogBySequence(sequence uint64) (*commonpb.Log, error) {
	kb := NewKeyBuilder()
	kb.PutByte(keyPrefixLog).
		PutUInt64(sequence)

	value, closer, err := s.db.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting system log by sequence: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	// Unmarshal protobuf Log
	log := &commonpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling system log from protobuf: %w", err)
	}

	return log, nil
}

// GetSequenceForIdempotencyKey retrieves the sequence for an idempotency key (global) (implements data.Store)
func (s *Store) GetSequenceForIdempotencyKey(idempotencyKey string) (uint64, error) {
	kb := NewKeyBuilder()
	kb.PutByte(keyPrefixIdempotency).
		PutString(idempotencyKey)

	value, closer, err := s.db.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("querying idempotency entry: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	return binary.BigEndian.Uint64(value[:8]), nil
}

// ListTransactionIDs returns a cursor over transaction IDs for a ledger (newest first).
// If afterTxID > 0, it starts after that transaction ID (exclusive).
// pageSize limits the number of results (0 = no limit).
func (s *Store) ListTransactionIDs(ledgerID uint32, pageSize uint32, afterTxID uint64) (Cursor[uint64], error) {
	kb := NewKeyBuilder()
	kb.PutLedgerPrefix(ledgerID).
		PutByte(keyPrefixTransactionUpdate)
	lowerBound := kb.Snapshot()

	// Calculate the offset where transaction ID starts in the key
	// Key format: [ledgerID (4 bytes)][keyPrefixTransactionUpdate (1 byte)][transactionID (8 bytes)][byLog (8 bytes)]
	txIDOffset := 4 + 1 // ledgerID (4 bytes) + 1 byte for keyPrefixTransactionUpdate

	// Upper bound: if afterTxID is specified, start from afterTxID - 1
	// Otherwise, start from the maximum possible transaction ID
	if afterTxID > 0 {
		kb.PutUInt64(afterTxID)
	} else {
		kb.PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	}
	upperBound := kb.Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for transaction list: %w", err)
	}

	return newTransactionIDCursor(iter, pageSize, txIDOffset), nil
}

// transactionIDCursor iterates over transaction IDs in reverse order (newest first)
type transactionIDCursor struct {
	iter       *pebble.Iterator
	started    bool
	pageSize   uint32
	count      uint32
	lastTxID   uint64 // Track last seen transaction ID to skip duplicates
	txIDOffset int    // Offset in key where transaction ID starts
}

func newTransactionIDCursor(iter *pebble.Iterator, pageSize uint32, txIDOffset int) *transactionIDCursor {
	return &transactionIDCursor{
		iter:       iter,
		pageSize:   pageSize,
		lastTxID:   ^uint64(0), // Max uint64 so first ID is always different
		txIDOffset: txIDOffset,
	}
}

func (c *transactionIDCursor) Next() (uint64, error) {
	// Check page limit
	if c.pageSize > 0 && c.count >= c.pageSize {
		return 0, io.EOF
	}

	for {
		var valid bool
		if !c.started {
			c.started = true
			valid = c.iter.Last()
		} else {
			valid = c.iter.Prev()
		}

		if !valid {
			if err := c.iter.Error(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}

		// Parse transaction ID from key
		// Key format: [ledgerName (variable)][keyPrefixTransactionUpdate (1 byte)][transactionID (8 bytes)][byLog (8 bytes)]
		key := c.iter.Key()

		// Ensure key is long enough to contain transaction ID
		if len(key) < c.txIDOffset+8 {
			continue
		}
		txID := binary.BigEndian.Uint64(key[c.txIDOffset : c.txIDOffset+8])

		// Skip if same as last transaction (duplicate entries for same tx)
		if txID == c.lastTxID {
			continue
		}

		c.lastTxID = txID
		c.count++
		return txID, nil
	}
}

func (c *transactionIDCursor) Close() error {
	return c.iter.Close()
}

// GetTransactionUpdates retrieves all updates for a transaction ID, ordered by ByLog.
func (s *Store) GetTransactionUpdates(ledgerID uint32, transactionID uint64) ([]*commonpb.TransactionUpdate, error) {
	kb := NewKeyBuilder()
	kb.PutLedgerPrefix(ledgerID).
		PutByte(keyPrefixTransactionUpdate).
		PutUInt64(transactionID)
	lowerBound := kb.Snapshot()

	// Upper bound: add 0xFF to get all entries for this transaction
	kb.PutByte(0xFF)
	upperBound := kb.Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for transaction updates: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var updates []*commonpb.TransactionUpdate

	for iter.First(); iter.Valid(); iter.Next() {
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading transaction update value: %w", err)
		}

		update := &commonpb.TransactionUpdate{}
		if err := proto.Unmarshal(valueBytes, update); err != nil {
			return nil, fmt.Errorf("unmarshaling transaction update: %w", err)
		}

		updates = append(updates, update)
	}

	return updates, nil
}

// CreateSnapshot creates a new checkpoint of the database and returns the checkpoint ID.
func (s *Store) CreateSnapshot() (uint64, error) {
	s.logger.Infof("Creating snapshot")

	newCheckpointID := s.currentCheckPoint + 1
	checkpointDir := filepath.Join(s.dataDir, "checkpoints", fmt.Sprintf("%d", newCheckpointID))
	if err := os.RemoveAll(checkpointDir); err != nil {
		return 0, fmt.Errorf("removing checkpoint directory: %w", err)
	}

	if err := s.db.Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
		return 0, fmt.Errorf("creating checkpoint: %w", err)
	}

	f, err := os.Create(filepath.Join(s.dataDir, currentCheckpointFile+".tmp"))
	if err != nil {
		return 0, fmt.Errorf("creating checkpoint file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := fmt.Fprintf(f, "%d", newCheckpointID); err != nil {
		return 0, fmt.Errorf("writing checkpoint file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("syncing checkpoint file: %w", err)
	}

	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("closing checkpoint file: %w", err)
	}

	if err := os.Rename(filepath.Join(s.dataDir, currentCheckpointFile+".tmp"), filepath.Join(s.dataDir, currentCheckpointFile)); err != nil {
		return 0, fmt.Errorf("renaming checkpoint file: %w", err)
	}

	// Clean up old checkpoints beyond the configured maximum
	// Note: it can fail, leaving old checkpoints on disk
	// this is not critical, but we should fix it eventually
	if err := s.cleanupOldCheckpoints(); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Infof("Failed to cleanup old checkpoints")
	}

	s.logger.WithFields(map[string]any{
		"checkpoint": newCheckpointID,
	}).Infof("Snapshot created")
	s.currentCheckPoint = newCheckpointID

	return newCheckpointID, nil
}

// cleanupOldCheckpoints removes checkpoints older than the configured maximum.
// It keeps the most recent maxCheckpoints checkpoints.
func (s *Store) cleanupOldCheckpoints() error {
	newCheckpoint := s.currentCheckPoint + 1

	// Calculate the oldest checkpoint to keep
	// If newCheckpoint is 15 and maxCheckpoints is 10, we keep checkpoints 6-15
	// So we delete anything older than (newCheckpoint - maxCheckpoints + 1)
	if newCheckpoint < uint64(s.maxCheckpoints) {
		// Not enough checkpoints yet, nothing to delete
		return nil
	}

	oldestToKeep := newCheckpoint - uint64(s.maxCheckpoints) + 1

	// Delete checkpoints from oldestCheckpoint up to (but not including) oldestToKeep
	for i := s.oldestCheckpoint; i < oldestToKeep; i++ {
		checkpointPath := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", i))
		if err := os.RemoveAll(checkpointPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing old checkpoint %d: %w", i, err)
		}
	}

	// Update the oldest checkpoint tracker
	s.oldestCheckpoint = oldestToKeep

	return nil
}

// GetCheckpointPath returns the filesystem path for a given checkpoint ID.
// Returns an error if the checkpoint does not exist.
func (s *Store) GetCheckpointPath(checkpointID uint64) (string, error) {
	path := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", checkpointID))
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("checkpoint %d not found", checkpointID)
		}
		return "", fmt.Errorf("checking checkpoint %d: %w", checkpointID, err)
	}
	return path, nil
}

// GetCurrentCheckpointID returns the current checkpoint ID.
func (s *Store) GetCurrentCheckpointID() uint64 {
	return s.currentCheckPoint
}

// PrepareCheckpointRestore prepares a directory for restoring a checkpoint from a remote peer.
// It returns the path to the directory where the checkpoint should be extracted.
func (s *Store) PrepareCheckpointRestore(checkpointID uint64) (string, error) {
	checkpointDir := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", checkpointID))

	// Remove any existing directory at this path
	if err := os.RemoveAll(checkpointDir); err != nil {
		return "", fmt.Errorf("removing existing checkpoint directory: %w", err)
	}

	// Create the directory
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return "", fmt.Errorf("creating checkpoint directory: %w", err)
	}

	return checkpointDir, nil
}

// RestoreCheckpoint restores the database from a checkpoint.
// This closes the current database, replaces it with the checkpoint, and reopens.
func (s *Store) RestoreCheckpoint(checkpointID uint64) error {
	checkpointDir := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", checkpointID))

	// Verify the checkpoint exists
	if _, err := os.Stat(checkpointDir); err != nil {
		return fmt.Errorf("checkpoint %d not found: %w", checkpointID, err)
	}

	// Close the current database
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("closing current database: %w", err)
	}

	// Remove the live directory
	liveDirectory := filepath.Join(s.dataDir, liveDir)
	if err := os.RemoveAll(liveDirectory); err != nil {
		return fmt.Errorf("removing live directory: %w", err)
	}

	// Hard link the checkpoint to the live directory
	if err := HardLink(checkpointDir, liveDirectory); err != nil {
		return fmt.Errorf("hard linking checkpoint to live directory: %w", err)
	}

	// Reopen the database with the same options
	db, err := pebble.Open(liveDirectory, s.opts)
	if err != nil {
		return fmt.Errorf("reopening database: %w", err)
	}
	s.db = db

	// Update the current checkpoint file
	f, err := os.Create(filepath.Join(s.dataDir, currentCheckpointFile))
	if err != nil {
		return fmt.Errorf("creating checkpoint file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := fmt.Fprintf(f, "%d", checkpointID); err != nil {
		return fmt.Errorf("writing checkpoint file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing checkpoint file: %w", err)
	}

	// Update internal state
	s.currentCheckPoint = checkpointID
	s.oldestCheckpoint = checkpointID

	s.logger.WithFields(map[string]any{
		"checkpointId": checkpointID,
	}).Infof("Database restored from checkpoint")

	return nil
}

func (s *Store) GetLastAppliedIndex() (uint64, error) {
	get, closer, err := s.db.Get([]byte{keyPrefixLastAppliedIndex})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer func() {
		_ = closer.Close()
	}()

	if len(get) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// GetLastSequence returns the last sequence number for system logs.
func (s *Store) GetLastSequence() (uint64, error) {
	kb := NewKeyBuilder()

	kb.PutByte(keyPrefixLog)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutByte(keyPrefixLog).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb.Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	// Seek to the last key in the range
	if !iter.Last() {
		// No system logs found
		return 0, nil
	}

	// Parse the sequence from the value (the system log protobuf contains the sequence)
	value, err := iter.ValueAndErr()
	if err != nil {
		return 0, fmt.Errorf("reading system log value: %w", err)
	}

	log := &commonpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return 0, fmt.Errorf("unmarshaling system log from protobuf: %w", err)
	}

	return log.Sequence, nil
}

// cursor implements Cursor[T] for Pebble where T is a proto.Message pointer.
type cursor[T proto.Message] struct {
	iter    *pebble.Iterator
	started bool
	elemTyp reflect.Type
}

func newCursor[T proto.Message](iter *pebble.Iterator) *cursor[T] {
	var zero T
	return &cursor[T]{
		iter:    iter,
		elemTyp: reflect.TypeOf(zero).Elem(),
	}
}

func (c *cursor[T]) Next() (T, error) {
	var zero T
	if !c.started {
		c.started = true
		if !c.iter.First() {
			if err := c.iter.Error(); err != nil {
				return zero, err
			}
			return zero, io.EOF
		}
	} else {
		if !c.iter.Next() {
			if err := c.iter.Error(); err != nil {
				return zero, err
			}
			return zero, io.EOF
		}
	}

	value, err := c.iter.ValueAndErr()
	if err != nil {
		return zero, fmt.Errorf("reading value: %w", err)
	}

	item := reflect.New(c.elemTyp).Interface().(T)
	if err := proto.Unmarshal(value, item); err != nil {
		return zero, fmt.Errorf("unmarshaling: %w", err)
	}
	return item, nil
}

func (c *cursor[T]) Close() error {
	if c.iter != nil {
		return c.iter.Close()
	}
	return nil
}

// ListLedgers returns a cursor over all registered ledgers.
func (s *Store) ListLedgers() (Cursor[*commonpb.LedgerInfo], error) {
	// Create bounds for ledger info prefix
	lowerBound := []byte{keyPrefixLedgerInfo}
	upperBound := []byte{keyPrefixLedgerInfo, 0xFF, 0xFF, 0xFF, 0xFF}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for ledger info: %w", err)
	}

	return newCursor[*commonpb.LedgerInfo](iter), nil
}

// GetLedgerByName retrieves a ledger by its name.
// Returns ErrNotFound if the ledger does not exist or is soft-deleted.
func (s *Store) GetLedgerByName(name string) (*commonpb.LedgerInfo, error) {
	cursor, err := s.ListLedgers()
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close() }()

	for {
		ledger, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if ledger.Name == name {
			// Check if soft-deleted
			if ledger.DeletedAt != nil {
				return nil, ErrNotFound
			}
			return ledger, nil
		}
	}

	return nil, ErrNotFound
}

// GetLedgerByID retrieves a ledger by its ID.
// Returns ErrNotFound if the ledger does not exist or is soft-deleted.
func (s *Store) GetLedgerByID(id uint32) (*commonpb.LedgerInfo, error) {
	// Build key: [keyPrefixLedgerInfo][ledgerID]
	key := make([]byte, 5)
	key[0] = keyPrefixLedgerInfo
	binary.BigEndian.PutUint32(key[1:], id)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("getting ledger by ID: %w", err)
	}
	defer func() { _ = closer.Close() }()

	info := &commonpb.LedgerInfo{}
	if err := proto.Unmarshal(value, info); err != nil {
		return nil, fmt.Errorf("unmarshaling ledger info: %w", err)
	}

	// Check if soft-deleted
	if info.DeletedAt != nil {
		return nil, ErrNotFound
	}

	return info, nil
}

func (s *Store) NewIter(p *pebble.IterOptions) (*pebble.Iterator, error) {
	return s.db.NewIter(p)
}

func HardLink(srcDir, dstDir string) error {
	srcDir = filepath.Clean(srcDir)
	dstDir = filepath.Clean(dstDir)

	srcInfo, err := os.Stat(srcDir)
	if err != nil {
		return fmt.Errorf("stat srcDir: %w", err)
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("srcDir is not a directory: %s", srcDir)
	}
	if _, err := os.Stat(dstDir); err == nil {
		return fmt.Errorf("dstDir already exists: %s", dstDir)
	}

	parent := filepath.Dir(dstDir)
	base := filepath.Base(dstDir)
	tmpDir := filepath.Join(parent, base+fmt.Sprintf(".tmp-%d-%d", os.Getpid(), time.Now().UnixNano()))

	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Create tmp root with same perms as source root (best effort).
	if err := os.MkdirAll(tmpDir, srcInfo.Mode().Perm()); err != nil {
		return fmt.Errorf("mkdir tmpDir: %w", err)
	}
	if err := fsyncDir(tmpDir); err != nil {
		return err
	}

	err = filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		dstPath := filepath.Join(tmpDir, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}

		switch {
		case info.IsDir():
			if err := os.MkdirAll(dstPath, info.Mode().Perm()); err != nil {
				return fmt.Errorf("mkdir %s: %w", dstPath, err)
			}
			_ = os.Chmod(dstPath, info.Mode().Perm())
			return fsyncDir(dstPath)

		case info.Mode().Type() == 0: // regular file
			if err := os.Link(path, dstPath); err != nil {
				return fmt.Errorf("hardlink %s -> %s: %w", path, dstPath, err)
			}
			_ = os.Chmod(dstPath, info.Mode().Perm())
			return nil

		case (info.Mode() & os.ModeSymlink) != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", path, err)
			}
			if err := os.Symlink(target, dstPath); err != nil {
				return fmt.Errorf("symlink %s -> %s: %w", dstPath, target, err)
			}
			return nil

		default:
			return fmt.Errorf("unsupported file type %s mode=%s", path, info.Mode().String())
		}
	})
	if err != nil {
		return fmt.Errorf("walk: %w", err)
	}

	if err := fsyncDir(tmpDir); err != nil {
		return err
	}
	if err := fsyncDir(parent); err != nil {
		return err
	}

	if err := os.Rename(tmpDir, dstDir); err != nil {
		return fmt.Errorf("rename publish: %w", err)
	}

	return fsyncDir(parent)
}

// fsyncDir fsyncs a directory so its entries are durably recorded. On Windows, it's a no-op.
func fsyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync %s: %w", dir, err)
	}
	defer func() {
		_ = f.Close()
	}()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}
