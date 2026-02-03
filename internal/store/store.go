package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

const (
	liveDir               = "live"
	currentCheckpointFile = "CURRENT_CHECKPOINT"
	checkpointsDir        = "checkpoints"
)

// Store is a Pebble implementation of store.Store
// It stores balances and account metadata
type Store struct {
	db                *pebble.DB
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
	keyPrefixBalanceDiff       byte = 0x04 // [ledger][keyPrefixBalanceDiff][account][asset][raftIndex] -> BigInt
	keyPrefixBalanceBase       byte = 0x05 // [ledger][keyPrefixBalanceBase][account][asset][raftIndex] -> BigInt
	keyPrefixMetadataDiff      byte = 0x06 // [ledger][keyPrefixMetadataDiff][account][key][raftIndex] -> value (string, empty = deleted)
	keyPrefixMetadataBase      byte = 0x07 // [ledger][keyPrefixMetadataBase][account][key][raftIndex] -> value (string, empty = deleted)
	keyPrefixTransactionUpdate byte = 0x08 // [ledger][keyPrefixTransactionUpdate][transactionID][byLog] -> TransactionUpdate
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

// logCursor implements Cursor[*commonpb.Log] for Pebble.
type logCursor struct {
	iter *pebble.Iterator
	s    *Store
}

func (c *logCursor) Next() (*commonpb.Log, error) {
	if !c.iter.Valid() {
		if err := c.iter.Error(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	// Read protobuf Log
	value, err := c.iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading log value: %w", err)
	}

	// Unmarshal protobuf Log
	log := &commonpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log from protobuf: %w", err)
	}

	// Move to next
	c.iter.Next()

	return log, nil
}

func (c *logCursor) Close() error {
	if c.iter != nil {
		return c.iter.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all system logs.
// Logs are returned in ascending order by sequence.
// from: optional sequence to start from (0 = from beginning).
// to: optional sequence to stop at (0 = until end, inclusive).
func (s *Store) GetAllLogs(from uint64, to uint64) (Cursor[*commonpb.Log], error) {
	kb := NewKeyBuilder()

	kb.PutByte(keyPrefixLog)
	if from > 0 {
		kb.PutUInt64(from)
	}
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutByte(keyPrefixLog)
	if to > 0 {
		kb.PutUInt64(to + 1)
	} else {
		kb.PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	}
	upperBound := kb.Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}

	// Position iterator at first key
	iter.First()

	return &logCursor{
		iter: iter,
		s:    s,
	}, nil
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

// ============================================================================
// Store Implementation
// ============================================================================

// GetBalanceDiffs retrieves all balance diffs for the given account/asset combinations.
// Returns the raw diffs with their RaftIndex - the caller is responsible for computing the final balance.
func (s *Store) GetBalanceDiffs(ledgerName string, query BalanceDiffsQuery) (BalanceDiffsResult, error) {
	result := make(BalanceDiffsResult)
	kb := NewKeyBuilder()

	// Build query for each account/asset combination
	for account, assets := range query {
		if len(assets) == 0 {
			continue
		}

		// Initialize account map
		result[account] = make(map[string][]StoredBalanceDiff)

		// Query each asset
		for _, asset := range assets {
			// Iterate over all balance diffs for this ledger/account/asset combination
			kb.PutLedgerPrefix(ledgerName).
				PutByte(keyPrefixBalanceDiff).
				PutString(account).
				PutString(asset)
			lowerBound := kb.Snapshot()

			kb.PutByte(0xFF)
			upperBound := kb.Build()

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			})
			if err != nil {
				return nil, fmt.Errorf("creating iterator for balance diffs: %w", err)
			}

			var diffs []StoredBalanceDiff

			// Collect all diffs
			for iter.First(); iter.Valid(); iter.Next() {
				key := iter.Key()
				valueBytes, err := iter.ValueAndErr()
				if err != nil {
					_ = iter.Close()
					return nil, fmt.Errorf("reading balance diff value: %w", err)
				}

				// Extract RaftIndex from key (last 8 bytes after lowerBound prefix)
				keyAfterPrefix := key[len(lowerBound):]
				if len(keyAfterPrefix) < 8 {
					_ = iter.Close()
					return nil, fmt.Errorf("malformed balance diff key: expected at least 8 bytes for RaftIndex, got %d", len(keyAfterPrefix))
				}
				raftIndex := binary.BigEndian.Uint64(keyAfterPrefix[:8])

				diff := &commonpb.BigInt{}
				if err := proto.Unmarshal(valueBytes, diff); err != nil {
					_ = iter.Close()
					return nil, fmt.Errorf("unmarshaling balance diff value: %w", err)
				}

				diffs = append(diffs, StoredBalanceDiff{
					RaftIndex: raftIndex,
					Diff:      diff,
				})
			}

			if err := iter.Close(); err != nil {
				return nil, fmt.Errorf("closing balance diff iterator: %w", err)
			}

			result[account][asset] = diffs
		}
	}

	return result, nil
}

// GetBalanceBase retrieves the most recent balance base for an account/asset pair
// whose RaftIndex is <= the given maxRaftIndex. Returns nil if no base exists.
func (s *Store) GetBalanceBase(ledgerName string, account, asset string, maxRaftIndex uint64) (*StoredBalanceBase, error) {
	kb := NewKeyBuilder()

	// Build the prefix for this ledger/account/asset
	kb.PutLedgerPrefix(ledgerName).
		PutByte(keyPrefixBalanceBase).
		PutString(account).
		PutString(asset)
	lowerBound := kb.Snapshot()

	// Upper bound includes maxRaftIndex + 1 so we include entries at maxRaftIndex
	kb.PutUInt64(maxRaftIndex + 1)
	upperBound := kb.Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for balance base: %w", err)
	}
	defer func() { _ = iter.Close() }()

	// Seek to the last entry in the range (the one with the highest RaftIndex <= maxRaftIndex)
	if !iter.Last() {
		// No base found
		return nil, nil
	}

	key := iter.Key()
	valueBytes, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading balance base value: %w", err)
	}

	// Extract RaftIndex from key (last 8 bytes after lowerBound prefix)
	keyAfterPrefix := key[len(lowerBound):]
	if len(keyAfterPrefix) < 8 {
		return nil, fmt.Errorf("malformed balance base key: expected at least 8 bytes for RaftIndex, got %d", len(keyAfterPrefix))
	}
	raftIndex := binary.BigEndian.Uint64(keyAfterPrefix[:8])

	balance := &commonpb.BigInt{}
	if err := proto.Unmarshal(valueBytes, balance); err != nil {
		return nil, fmt.Errorf("unmarshaling balance base value: %w", err)
	}

	return &StoredBalanceBase{
		RaftIndex: raftIndex,
		Balance:   balance,
	}, nil
}

// GetMetadataBase retrieves the most recent metadata base for an account/key pair
// whose RaftIndex is <= the given maxRaftIndex. Returns nil if no base exists.
func (s *Store) GetMetadataBase(ledgerName string, account, key string, maxRaftIndex uint64) (*StoredMetadataBase, error) {
	kb := NewKeyBuilder()

	// Build the prefix for this ledger/account/key
	kb.PutLedgerPrefix(ledgerName).
		PutByte(keyPrefixMetadataBase).
		PutString(account).
		PutString(key)
	lowerBound := kb.Snapshot()

	// Upper bound includes maxRaftIndex + 1 so we include entries at maxRaftIndex
	kb.PutUInt64(maxRaftIndex + 1)
	upperBound := kb.Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for metadata base: %w", err)
	}
	defer func() { _ = iter.Close() }()

	// Seek to the last entry in the range (the one with the highest RaftIndex <= maxRaftIndex)
	if !iter.Last() {
		// No base found
		return nil, nil
	}

	iterKey := iter.Key()
	valueBytes, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading metadata base value: %w", err)
	}

	// Extract RaftIndex from key (last 8 bytes after lowerBound prefix)
	keyAfterPrefix := iterKey[len(lowerBound):]
	if len(keyAfterPrefix) < 8 {
		return nil, fmt.Errorf("malformed metadata base key: expected at least 8 bytes for RaftIndex, got %d", len(keyAfterPrefix))
	}
	raftIndex := binary.BigEndian.Uint64(keyAfterPrefix[:8])

	// Empty value means deleted
	var value *commonpb.MetadataValue
	if len(valueBytes) > 0 {
		value = &commonpb.MetadataValue{}
		if err := proto.Unmarshal(valueBytes, value); err != nil {
			return nil, fmt.Errorf("unmarshaling metadata base value: %w", err)
		}
	}

	return &StoredMetadataBase{
		RaftIndex: raftIndex,
		Value:     value,
	}, nil
}

// GetAccountMetadata retrieves account metadata for multiple accounts from Pebble for a specific ledger (implements store.Store)
func (s *Store) GetAccountMetadata(ledgerName string, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata)
	kb := NewKeyBuilder()

	// Initialize with empty metadata for all requested accounts
	for _, account := range accounts {
		result[account] = make(metadata.Metadata)
	}

	// For each account, we need to find the latest value for each key
	for _, account := range accounts {
		// Track the latest state for each key: map[key] -> (raftIndex, value)
		// nil value means deletion
		keyStates := make(map[string]struct {
			raftIndex uint64
			value     *commonpb.MetadataValue
		})

		// Read metadata diffs (empty value = deletion)
		kb.PutLedgerPrefix(ledgerName).
			PutByte(keyPrefixMetadataDiff).
			PutString(account)
		lowerBound := kb.Snapshot()
		kb.PutByte(0xFF)
		upperBound := kb.Build()

		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		})
		if err != nil {
			return nil, fmt.Errorf("creating metadata iterator for ledger %s account %s: %w", ledgerName, account, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// Extract metadata key and raft index from full key
			// Format: [ledger_prefix][keyPrefixMetadataDiff][account][key][raftIndex(8 bytes)]
			keyAfterPrefix := key[len(lowerBound):]
			if len(keyAfterPrefix) <= 8 {
				continue // Skip malformed keys (must have at least key + 8 bytes for raftIndex)
			}
			// Metadata key is everything except the last 8 bytes (which is the raftIndex)
			metadataKey := string(keyAfterPrefix[:len(keyAfterPrefix)-8])
			raftIndex := binary.BigEndian.Uint64(keyAfterPrefix[len(keyAfterPrefix)-8:])

			valueBytes, err := iter.ValueAndErr()
			if err != nil {
				_ = iter.Close()
				return nil, fmt.Errorf("reading metadata diff value: %w", err)
			}

			var metadataValue *commonpb.MetadataValue
			if len(valueBytes) > 0 {
				metadataValue = &commonpb.MetadataValue{}
				if err := proto.Unmarshal(valueBytes, metadataValue); err != nil {
					_ = iter.Close()
					return nil, fmt.Errorf("unmarshaling metadata diff value: %w", err)
				}
			}

			state, exists := keyStates[metadataKey]
			if !exists || raftIndex > state.raftIndex {
				keyStates[metadataKey] = struct {
					raftIndex uint64
					value     *commonpb.MetadataValue
				}{raftIndex: raftIndex, value: metadataValue}
			}
		}
		_ = iter.Close()

		// Build final metadata from key states (excluding deleted keys - nil value)
		for metadataKey, state := range keyStates {
			if state.value != nil {
				result[account][metadataKey] = state.value.Value
			}
		}
	}

	return result, nil
}

// GetAccountVolumes retrieves all volumes (input, output, balance) for all assets of an account
// Input is calculated as sum of positive balance diffs (when account receives funds)
// Output is calculated as sum of absolute negative balance diffs (when account sends funds)
func (s *Store) GetAccountVolumes(ledgerName string, account string) (map[string]*commonpb.VolumesWithBalance, error) {
	result := make(map[string]*commonpb.VolumesWithBalance)
	kb := NewKeyBuilder()

	// Build prefix for all balance diffs for this account (across all assets)
	kb.PutLedgerPrefix(ledgerName).
		PutByte(keyPrefixBalanceDiff).
		PutString(account)
	lowerBound := kb.Snapshot()

	kb.PutByte(0xFF)
	upperBound := kb.Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for account volumes: %w", err)
	}
	defer func() { _ = iter.Close() }()

	// Track input/output per asset
	inputByAsset := make(map[string]*big.Int)
	outputByAsset := make(map[string]*big.Int)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading balance diff value: %w", err)
		}

		// Extract asset from key (format: ledger_prefix + keyPrefixBalanceDiff + account + asset + sequence)
		// After lowerBound, the remaining bytes are: asset + sequence (8 bytes)
		keyAfterPrefix := key[len(lowerBound):]
		if len(keyAfterPrefix) <= 8 {
			continue // Skip malformed keys (must have at least asset + 8 bytes for sequence)
		}
		// Asset is everything except the last 8 bytes (which is the sequence)
		asset := string(keyAfterPrefix[:len(keyAfterPrefix)-8])

		// Initialize if needed
		if _, exists := inputByAsset[asset]; !exists {
			inputByAsset[asset] = big.NewInt(0)
			outputByAsset[asset] = big.NewInt(0)
		}

		// Parse the diff value
		diff := &commonpb.BigInt{}
		if err := proto.Unmarshal(valueBytes, diff); err != nil {
			return nil, fmt.Errorf("unmarshaling balance diff value: %w", err)
		}
		diffValue := diff.Value()

		// Positive diff = input (receiving funds), negative diff = output (sending funds)
		if diffValue.Sign() > 0 {
			inputByAsset[asset] = inputByAsset[asset].Add(inputByAsset[asset], diffValue)
		} else if diffValue.Sign() < 0 {
			// Output is the absolute value of negative diffs
			outputByAsset[asset] = outputByAsset[asset].Sub(outputByAsset[asset], diffValue)
		}
	}

	// Build result
	for asset := range inputByAsset {
		input := inputByAsset[asset]
		output := outputByAsset[asset]
		balance := new(big.Int).Sub(input, output)

		result[asset] = &commonpb.VolumesWithBalance{
			Input:   input.String(),
			Output:  output.String(),
			Balance: balance.String(),
		}
	}

	return result, nil
}

// GetSequenceForIdempotencyKey retrieves the sequence for an idempotency key (global) (implements store.Store)
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

// GetSequenceForTransactionID retrieves the sequence (ByLog of TransactionInit) for a given transaction ID.
// Returns 0 if no TransactionInit is found.
func (s *Store) GetSequenceForTransactionID(ledgerName string, transactionID uint64) (uint64, error) {
	updates, err := s.GetTransactionUpdates(ledgerName, transactionID)
	if err != nil {
		return 0, err
	}

	// Find the TransactionInit update
	for _, update := range updates {
		for _, updateType := range update.Updates {
			if updateType.GetTransactionInit() != nil {
				return update.ByLog, nil
			}
		}
	}

	return 0, nil
}

// IsTransactionReverted checks if a transaction has been reverted for a specific ledger.
// Returns true if a TransactionUpdateRevert exists for this transaction.
func (s *Store) IsTransactionReverted(ledgerName string, transactionID uint64) (bool, error) {
	updates, err := s.GetTransactionUpdates(ledgerName, transactionID)
	if err != nil {
		return false, err
	}

	// Check for any revert update
	for _, update := range updates {
		for _, updateType := range update.Updates {
			if updateType.GetTransactionModificationRevert() != nil {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetTransactionUpdates retrieves all updates for a transaction ID, ordered by ByLog.
func (s *Store) GetTransactionUpdates(ledgerName string, transactionID uint64) ([]*commonpb.TransactionUpdate, error) {
	kb := NewKeyBuilder()
	kb.PutLedgerPrefix(ledgerName).
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
