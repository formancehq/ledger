package service

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// ============================================================================
// Pebble Runtime Store Implementation
// ============================================================================

// PebbleRuntimeStore is a Pebble implementation of RuntimeStore
// It stores balances and account metadata
type PebbleRuntimeStore struct {
	db     *pebble.DB
	logger logging.Logger
}

// Key prefixes for Pebble storage (3 bytes max)
var (
	keyPrefixLog             = []byte("l")
	keyPrefixLogIdempotency  = []byte("lid")
	keyPrefixBalance         = []byte("bal")
	keyPrefixAccountMetadata = []byte("met")
	keyPrefixIdempotency     = []byte("idm")
	keyPrefixTransactionID   = []byte("tid")
)

// logKey returns the key for a log entry.
func logKey(id uint64) []byte {
	// Use big-endian encoding for proper lexicographic ordering
	key := make([]byte, len(keyPrefixLog)+8)
	copy(key, keyPrefixLog)
	binary.BigEndian.PutUint64(key[len(keyPrefixLog):], id)
	return key
}

// NewPebbleRuntimeStore creates a new PebbleRuntimeStore instance
func NewPebbleRuntimeStore(
	dataDir string,
	logger logging.Logger,
	meter metric.Meter,
) (*PebbleRuntimeStore, error) {
	// Create data directory if it doesn't exist
	dbPath := filepath.Join(dataDir, "runtime")

	opts := &pebble.Options{
		EventListener: NewPebbleMetricsListener(meter),
		MaxConcurrentCompactions: func() int {
			return 3
		},
		MemTableSize: 1 << 23, // 8 mb
	}
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble database: %w", err)
	}

	store := &PebbleRuntimeStore{
		db:     db,
		logger: logger,
	}

	return store, nil
}

// Close closes the Pebble database
func (s *PebbleRuntimeStore) Close() error {
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

// ============================================================================
// RuntimeStore InsertLogs Implementation
// ============================================================================

// InsertLogs persists logs and updates runtime state.
func (s *PebbleRuntimeStore) InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	if len(logs) == 0 {
		return nil
	}

	update, err := LogsToRuntimeUpdate(logs)
	if err != nil {
		return err
	}

	// Use a batch for atomic inserts
	batch := s.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	for _, log := range logs {
		// Validate log data
		if log.Data == nil {
			return fmt.Errorf("log data is nil for id %d", log.Id)
		}

		// Marshal protobuf Log to binary
		logBinary, err := proto.Marshal(log)
		if err != nil {
			return fmt.Errorf("marshaling log to protobuf: %w", err)
		}

		// Use log ID as key for efficient lookups
		key := logKey(log.Id)
		if err := batch.Set(key, logBinary, pebble.NoSync); err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}

		// Also create an index by idempotency key if present
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			keyBytes := []byte(log.Idempotency.Key)
			idempotencyKey := make([]byte, len(keyPrefixLogIdempotency)+len(keyBytes))
			copy(idempotencyKey, keyPrefixLogIdempotency)
			copy(idempotencyKey[len(keyPrefixLogIdempotency):], keyBytes)
			// Store the log ID as value for quick lookup
			idValue := make([]byte, 8)
			binary.BigEndian.PutUint64(idValue, log.Id)
			if err := batch.Set(idempotencyKey, idValue, pebble.NoSync); err != nil {
				return fmt.Errorf("inserting idempotency index: %w", err)
			}
		}
	}

	if err := s.applyRuntimeUpdate(batch, update); err != nil {
		return err
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Logs inserted into Pebble")
	return nil
}

func (s *PebbleRuntimeStore) applyRuntimeUpdate(batch *pebble.Batch, update RuntimeUpdate) error {
	// Apply balance differences
	// Store each diff as a separate entry instead of reading and updating the current balance
	if len(update.BalanceDiffs) > 0 {
		for account, assets := range update.BalanceDiffs {
			for asset, diff := range assets {
				// Generate a unique key for this diff using timestamp
				// Format: balance:{account}:{asset}:{timestamp_nanoseconds}
				key := balanceDiffKey(account, asset, time.Now().UnixNano())

				// Store the diff directly without reading the current balance
				if err := batch.Set(key, diff.Bytes(), pebble.NoSync); err != nil {
					return fmt.Errorf("storing balance diff for account %s asset %s: %w", account, asset, err)
				}
			}
		}
	}

	// Apply account metadata updates
	// Metadata values are always strings, so we store them directly
	if len(update.AccountMetadata) > 0 {
		for accountAddr, metadataMap := range update.AccountMetadata {
			for key, value := range metadataMap {
				pebbleKey := accountMetadataKey(accountAddr, key)
				if err := batch.Set(pebbleKey, []byte(value), pebble.NoSync); err != nil {
					return fmt.Errorf("upserting account metadata: %w", err)
				}
			}
		}
	}

	// Apply account metadata deletions
	if len(update.AccountMetadataDeletes) > 0 {
		for accountAddr, keys := range update.AccountMetadataDeletes {
			for _, key := range keys {
				pebbleKey := accountMetadataKey(accountAddr, key)
				if err := batch.Delete(pebbleKey, pebble.NoSync); err != nil {
					return fmt.Errorf("deleting account metadata key: %w", err)
				}
			}
		}
	}

	// Apply idempotency entries
	if len(update.IdempotencyKeys) > 0 {
		for key, entry := range update.IdempotencyKeys {
			// Store idempotency entry as protobuf
			data, err := proto.Marshal(entry)
			if err != nil {
				return fmt.Errorf("marshaling idempotency entry: %w", err)
			}

			pebbleKey := idempotencyPebbleKey(key)
			if err := batch.Set(pebbleKey, data, pebble.NoSync); err != nil {
				return fmt.Errorf("inserting idempotency entry for key %s: %w", key, err)
			}
		}
	}

	// Apply transaction ID to log ID mappings
	if len(update.TransactionIDs) > 0 {
		for transactionID, logID := range update.TransactionIDs {
			pebbleKey := transactionIDPebbleKey(transactionID)
			// Store log ID as 8-byte big-endian value
			logIDValue := make([]byte, 8)
			binary.BigEndian.PutUint64(logIDValue, logID)
			if err := batch.Set(pebbleKey, logIDValue, pebble.NoSync); err != nil {
				return fmt.Errorf("inserting transaction ID mapping for transaction %d: %w", transactionID, err)
			}
		}
	}

	return nil
}

// ============================================================================
// LogReader Implementation
// ============================================================================

// pebbleLogCursor implements Cursor[*ledgerpb.Log] for Pebble.
type pebbleLogCursor struct {
	iter  *pebble.Iterator
	store *PebbleRuntimeStore
}

func (c *pebbleLogCursor) Next(ctx context.Context) (*ledgerpb.Log, error) {
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
	log := &ledgerpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log from protobuf: %w", err)
	}

	// Move to next
	c.iter.Next()

	return log, nil
}

func (c *pebbleLogCursor) Close() error {
	if c.iter != nil {
		return c.iter.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs.
// Logs are returned in ascending order by id.
// from: optional log id to start from (0 = from beginning).
// to: optional log id to stop at (0 = until end, inclusive).
func (s *PebbleRuntimeStore) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	// Set up iterator bounds
	lowerBound := make([]byte, len(keyPrefixLog))
	copy(lowerBound, keyPrefixLog)
	upperBound := make([]byte, len(keyPrefixLog)+8)
	copy(upperBound, keyPrefixLog)
	for i := len(keyPrefixLog); i < len(upperBound); i++ {
		upperBound[i] = 0xFF
	}

	if from > 0 {
		lowerBound = logKey(from)
	}
	if to > 0 {
		// Upper bound should be exclusive, so we use to+1
		upperBound = logKey(to + 1)
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}

	// Position iterator at first key
	iter.First()

	return &pebbleLogCursor{
		iter:  iter,
		store: s,
	}, nil
}

// GetLogByID retrieves a log by its ID.
func (s *PebbleRuntimeStore) GetLogByID(ctx context.Context, id uint64) (*ledgerpb.Log, error) {
	return s.GetLogWithID(ctx, id)
}

// GetLogWithID retrieves a log by its ID.
func (s *PebbleRuntimeStore) GetLogWithID(ctx context.Context, id uint64) (*ledgerpb.Log, error) {
	key := logKey(id)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting log by id: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	// Unmarshal protobuf Log
	log := &ledgerpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log from protobuf: %w", err)
	}

	return log, nil
}

// GetLastProcessedLogID retrieves the ID of the last inserted log.
func (s *PebbleRuntimeStore) GetLastProcessedLogID(ctx context.Context) (uint64, error) {
	upperBound := make([]byte, len(keyPrefixLog)+8)
	copy(upperBound, keyPrefixLog)
	for i := len(keyPrefixLog); i < len(upperBound); i++ {
		upperBound[i] = 0xFF
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyPrefixLog,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, fmt.Errorf("getting last log: %w", err)
		}
		return 0, nil
	}

	key := iter.Key()
	if len(key) < len(keyPrefixLog)+8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(key[len(keyPrefixLog):]), nil
}

// balanceDiffKey returns a unique key for a balance diff entry
// Format: bal{account}:{asset}:{timestamp}
func balanceDiffKey(account, asset string, timestamp int64) []byte {
	accountBytes := []byte(account)
	assetBytes := []byte(asset)
	// Calculate size: prefix(3) + account + 1(:) + asset + 1(:) + timestamp(8)
	size := len(keyPrefixBalance) + len(accountBytes) + 1 + len(assetBytes) + 1 + 8
	key := make([]byte, size)
	offset := 0
	copy(key[offset:], keyPrefixBalance)
	offset += len(keyPrefixBalance)
	copy(key[offset:], accountBytes)
	offset += len(accountBytes)
	key[offset] = ':'
	offset++
	copy(key[offset:], assetBytes)
	offset += len(assetBytes)
	key[offset] = ':'
	offset++
	binary.BigEndian.PutUint64(key[offset:], uint64(timestamp))
	return key
}

// balancePrefix returns the prefix for all balance diffs for a given account/asset
// Format: bal{account}:{asset}:
func balancePrefix(account, asset string) []byte {
	accountBytes := []byte(account)
	assetBytes := []byte(asset)
	// Calculate size: prefix(3) + account + 1(:) + asset + 1(:)
	size := len(keyPrefixBalance) + len(accountBytes) + 1 + len(assetBytes) + 1
	key := make([]byte, size)
	offset := 0
	copy(key[offset:], keyPrefixBalance)
	offset += len(keyPrefixBalance)
	copy(key[offset:], accountBytes)
	offset += len(accountBytes)
	key[offset] = ':'
	offset++
	copy(key[offset:], assetBytes)
	offset += len(assetBytes)
	key[offset] = ':'
	return key
}

// accountMetadataKey returns the key for an account metadata entry
// Format: met{account}:{key}
func accountMetadataKey(account, key string) []byte {
	accountBytes := []byte(account)
	keyBytes := []byte(key)
	// Calculate size: prefix(3) + account + 1(:) + key
	size := len(keyPrefixAccountMetadata) + len(accountBytes) + 1 + len(keyBytes)
	result := make([]byte, size)
	offset := 0
	copy(result[offset:], keyPrefixAccountMetadata)
	offset += len(keyPrefixAccountMetadata)
	copy(result[offset:], accountBytes)
	offset += len(accountBytes)
	result[offset] = ':'
	offset++
	copy(result[offset:], keyBytes)
	return result
}

// idempotencyPebbleKey returns the key for an idempotency entry
// Format: idm{key}
func idempotencyPebbleKey(key string) []byte {
	keyBytes := []byte(key)
	result := make([]byte, len(keyPrefixIdempotency)+len(keyBytes))
	copy(result, keyPrefixIdempotency)
	copy(result[len(keyPrefixIdempotency):], keyBytes)
	return result
}

// transactionIDPebbleKey returns the key for a transaction ID to log ID mapping
// Format: tid{transactionID}
func transactionIDPebbleKey(transactionID uint64) []byte {
	// Use big-endian encoding for proper lexicographic ordering
	key := make([]byte, len(keyPrefixTransactionID)+8)
	copy(key, keyPrefixTransactionID)
	binary.BigEndian.PutUint64(key[len(keyPrefixTransactionID):], transactionID)
	return key
}

// ============================================================================
// RuntimeStore Implementation
// ============================================================================

// GetBalances retrieves balances from Pebble (implements RuntimeStore)
// Sums all balance diffs for each account/asset combination
func (s *PebbleRuntimeStore) GetBalances(ctx context.Context, balanceQuery map[string][]string) (ledgerpb.Balances, error) {
	result := make(ledgerpb.Balances)

	// If no query provided, return empty balances
	if len(balanceQuery) == 0 {
		return result, nil
	}

	// Build query for each account/asset combination
	for account, assets := range balanceQuery {
		if len(assets) == 0 {
			continue
		}

		// Initialize account map
		result[account] = make(map[string]*big.Int)

		// Query each asset
		for _, asset := range assets {
			// Initialize balance to zero
			balance := big.NewInt(0)

			// Iterate over all balance diffs for this account/asset combination
			prefix := balancePrefix(account, asset)
			upperBound := append(prefix, 0xFF)

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: prefix,
				UpperBound: upperBound,
			})
			if err != nil {
				return nil, fmt.Errorf("creating iterator for balance diffs: %w", err)
			}

			// Sum all diffs
			for iter.First(); iter.Valid(); iter.Next() {
				valueBytes, err := iter.ValueAndErr()
				if err != nil {
					_ = iter.Close()
					return nil, fmt.Errorf("reading balance diff value: %w", err)
				}

				diff := new(big.Int).SetBytes(valueBytes)
				balance = balance.Add(balance, diff)
			}

			if err := iter.Close(); err != nil {
				return nil, fmt.Errorf("closing balance diff iterator: %w", err)
			}

			result[account][asset] = balance
		}
	}

	return result, nil
}

// GetAccountMetadata retrieves account metadata for multiple accounts from Pebble (implements RuntimeStore)
func (s *PebbleRuntimeStore) GetAccountMetadata(ctx context.Context, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata)

	// If no accounts requested, return empty map
	if len(accounts) == 0 {
		return result, nil
	}

	// Initialize with empty metadata for all requested accounts
	for _, account := range accounts {
		result[account] = make(metadata.Metadata)
	}

	// Iterate over all accounts and read their metadata
	for _, account := range accounts {
		accountBytes := []byte(account)
		prefix := make([]byte, len(keyPrefixAccountMetadata)+len(accountBytes)+1)
		copy(prefix, keyPrefixAccountMetadata)
		copy(prefix[len(keyPrefixAccountMetadata):], accountBytes)
		prefix[len(prefix)-1] = ':'

		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: append(prefix, 0xFF),
		})
		if err != nil {
			return nil, fmt.Errorf("creating iterator for account %s: %w", account, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// Extract metadata key from full key (format: "met{account}:{key}")
			if len(key) > len(prefix) {
				metadataKeyBytes := key[len(prefix):]
				metadataKey := string(metadataKeyBytes)
				valueBytes, err := iter.ValueAndErr()
				if err != nil {
					return nil, fmt.Errorf("reading metadata value for account %s key %s: %w", account, metadataKey, err)
				}

				// Metadata values are stored as strings directly
				valueStr := string(valueBytes)
				result[account][metadataKey] = valueStr
			}
		}
		_ = iter.Close()
	}

	return result, nil
}

// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key (implements RuntimeStore)
func (s *PebbleRuntimeStore) GetLogForIdempotencyKey(ctx context.Context, idempotencyKey string) ([]byte, uint64, error) {
	if idempotencyKey == "" {
		return nil, 0, nil
	}

	key := idempotencyPebbleKey(idempotencyKey)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("querying idempotency entry: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	// Parse protobuf
	var idempotencyProto ledgerpb.IdempotencyEntry
	if err := proto.Unmarshal(value, &idempotencyProto); err != nil {
		return nil, 0, fmt.Errorf("unmarshaling idempotency entry: %w", err)
	}

	return idempotencyProto.Hash, idempotencyProto.LogId, nil
}

// GetLogIDForTransactionID retrieves the log ID for a given transaction ID (implements RuntimeStore)
func (s *PebbleRuntimeStore) GetLogIDForTransactionID(ctx context.Context, transactionID uint64) (uint64, error) {
	if transactionID == 0 {
		return 0, nil
	}

	key := transactionIDPebbleKey(transactionID)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("querying transaction ID mapping: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	// Parse log ID from 8-byte big-endian value
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid log ID value length: expected 8 bytes, got %d", len(value))
	}
	logID := binary.BigEndian.Uint64(value)

	return logID, nil
}

// Metrics returns Pebble database metrics (implements MetricsAware)
func (s *PebbleRuntimeStore) Metrics() any {
	return s.db.Metrics()
}
