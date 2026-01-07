package service

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"

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

// Key prefixes for Pebble storage
const (
	keyPrefixBalance         = "balance:"
	keyPrefixAccountMetadata = "metadata:"
	keyPrefixIdempotency     = "idempotency:"
	keyLastProcessedLogID    = "info:last_processed_log_id"
)

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
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// ============================================================================
// RuntimeStore Update Implementation
// ============================================================================

// Update applies all runtime updates atomically
func (s *PebbleRuntimeStore) Update(ctx context.Context, update RuntimeUpdate) error {
	// Use a batch for atomic updates
	batch := s.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	// Apply balance differences
	if len(update.BalanceDiffs) > 0 {
		for account, assets := range update.BalanceDiffs {
			for asset, diff := range assets {
				key := balanceKey(account, asset)

				// Read current balance
				currentBalance := big.NewInt(0)
				value, closer, err := s.db.Get(key)
				if err != nil && !errors.Is(err, pebble.ErrNotFound) {
					return fmt.Errorf("reading balance for account %s asset %s: %w", account, asset, err)
				}
				if err == nil {
					currentBalance = new(big.Int).SetBytes(value)
					if err := closer.Close(); err != nil {
						return fmt.Errorf("closing balance reader: %w", err)
					}
				}

				// Calculate new balance
				newBalance := currentBalance.Add(currentBalance, diff)

				// Write new balance to batch
				if err := batch.Set(key, newBalance.Bytes(), pebble.NoSync); err != nil {
					return fmt.Errorf("updating balance for account %s asset %s: %w", account, asset, err)
				}
			}
		}
	}

	// Apply account metadata updates
	// Metadata values are always strings, so we store them directly
	if len(update.AccountMetadata) > 0 {
		for accountAddr, metadataMap := range update.AccountMetadata {
			for key, value := range metadataMap {
				// Convert value to string (metadata values are always strings)
				var valueStr string
				if strVal, ok := value.(string); ok {
					valueStr = strVal
				} else {
					// Fallback: convert to string representation
					valueStr = fmt.Sprintf("%v", value)
				}

				pebbleKey := accountMetadataKey(accountAddr, key)
				if err := batch.Set(pebbleKey, []byte(valueStr), pebble.NoSync); err != nil {
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
			idempotencyProto := &ledgerpb.IdempotencyEntry{
				Hash:  entry.Hash,
				LogId: entry.LogID,
			}
			data, err := proto.Marshal(idempotencyProto)
			if err != nil {
				return fmt.Errorf("marshaling idempotency entry: %w", err)
			}

			pebbleKey := idempotencyPebbleKey(key)
			if err := batch.Set(pebbleKey, data, pebble.NoSync); err != nil {
				return fmt.Errorf("inserting idempotency entry for key %s: %w", key, err)
			}
		}
	}

	// Update last processed log ID
	if update.LastProcessedLogID > 0 {
		value := fmt.Sprintf("%d", update.LastProcessedLogID)
		key := []byte(keyLastProcessedLogID)
		if err := batch.Set(key, []byte(value), pebble.NoSync); err != nil {
			return fmt.Errorf("updating last processed log ID: %w", err)
		}
	}

	// Commit the batch
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	return nil
}

// balanceKey returns the key for a balance entry
func balanceKey(account, asset string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s", keyPrefixBalance, account, asset))
}

// accountMetadataKey returns the key for an account metadata entry
func accountMetadataKey(account, key string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s", keyPrefixAccountMetadata, account, key))
}

// idempotencyPebbleKey returns the key for an idempotency entry
func idempotencyPebbleKey(key string) []byte {
	return []byte(fmt.Sprintf("%s%s", keyPrefixIdempotency, key))
}

// ============================================================================
// RuntimeStore Implementation
// ============================================================================

// GetBalances retrieves balances from Pebble (implements RuntimeStore)
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
			key := balanceKey(account, asset)
			value, closer, err := s.db.Get(key)
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return nil, fmt.Errorf("querying balance: %w", err)
			}

			if err == nil {
				balance := new(big.Int).SetBytes(value)
				if err := closer.Close(); err != nil {
					return nil, fmt.Errorf("closing balance reader: %w", err)
				}
				result[account][asset] = balance
			} else {
				// Asset doesn't exist, set zero balance
				result[account][asset] = big.NewInt(0)
			}
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
		prefix := []byte(fmt.Sprintf("%s%s:", keyPrefixAccountMetadata, account))

		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: append(prefix, 0xFF),
		})
		if err != nil {
			return nil, fmt.Errorf("creating iterator for account %s: %w", account, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// Extract metadata key from full key (format: "metadata:account:key")
			keyStr := string(key)
			if len(keyStr) > len(prefix) {
				metadataKey := keyStr[len(prefix):]
				valueBytes := iter.Value()

				// Metadata values are stored as strings directly
				valueStr := string(valueBytes)
				result[account][metadataKey] = valueStr
			}
		}
		iter.Close()
	}

	return result, nil
}

// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key (implements RuntimeStore)
func (s *PebbleRuntimeStore) GetLogForIdempotencyKey(ctx context.Context, idempotencyKey string) (string, uint64, error) {
	if idempotencyKey == "" {
		return "", 0, nil
	}

	key := idempotencyPebbleKey(idempotencyKey)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return "", 0, nil
		}
		return "", 0, fmt.Errorf("querying idempotency entry: %w", err)
	}
	defer closer.Close()

	// Parse protobuf
	var idempotencyProto ledgerpb.IdempotencyEntry
	if err := proto.Unmarshal(value, &idempotencyProto); err != nil {
		return "", 0, fmt.Errorf("unmarshaling idempotency entry: %w", err)
	}

	return idempotencyProto.Hash, idempotencyProto.LogId, nil
}

// GetLastProcessedLogID retrieves the ID of the last processed log from Pebble
func (s *PebbleRuntimeStore) GetLastProcessedLogID(ctx context.Context) (uint64, error) {
	key := []byte(keyLastProcessedLogID)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, fmt.Errorf("querying last processed log ID: %w", err)
	}
	defer closer.Close()

	// Parse the value as uint64
	var lastLogID uint64
	if _, err := fmt.Sscanf(string(value), "%d", &lastLogID); err != nil {
		return 0, fmt.Errorf("parsing last processed log ID: %w", err)
	}

	return lastLogID, nil
}
