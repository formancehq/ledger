package pebble

import (
	"bytes"
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
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

// ============================================================================
// Pebble Runtime Store Implementation
// ============================================================================

// RuntimeStore is a Pebble implementation of store.RuntimeStore
// It stores balances and account metadata
type RuntimeStore struct {
	db     *pebble.DB
	logger logging.Logger
}

// Key prefixes for Pebble storage (3 bytes max)
var (
	keyPrefixLog             byte = 0x01
	keyPrefixBalanceDiff     byte = 0x02
	keyPrefixAccountMetadata byte = 0x03
	keyPrefixIdempotency     byte = 0x04
	keyPrefixTransactionID   byte = 0x05
	keyPrefixRevertedTxID    byte = 0x06
)

// NewRuntimeStore creates a new RuntimeStore instance
func NewRuntimeStore(
	dataDir string,
	logger logging.Logger,
	meter metric.Meter,
) (*RuntimeStore, error) {
	// Create data directory if it doesn't exist
	dbPath := filepath.Join(dataDir, "runtime")

	opts := &pebble.Options{
		EventListener: NewMetricsListener(meter),
		MaxConcurrentCompactions: func() int {
			return 3
		},
		MemTableSize: 1 << 25, // 32 MB
	}
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble database: %w", err)
	}

	s := &RuntimeStore{
		db:     db,
		logger: logger,
	}

	return s, nil
}

// Close closes the Pebble database
func (s *RuntimeStore) Close(ctx context.Context) error {
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
func (s *RuntimeStore) InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	if len(logs) == 0 {
		return nil
	}

	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	update, err := store.LogsToRuntimeUpdate(logs)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	for _, log := range logs {
		// Marshal protobuf Log to binary
		logBinary, err := proto.Marshal(log)
		if err != nil {
			return fmt.Errorf("marshaling log to protobuf: %w", err)
		}

		writeLedgerPrefix(buf, log.Ledger)
		writeByte(buf, keyPrefixLog)
		writeUInt64(buf, log.Id)

		if err := setOnBatch(batch, buf, logBinary); err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}

		// Also create an index by idempotency key if present
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			writeLedgerPrefix(buf, log.Ledger)
			writeByte(buf, keyPrefixIdempotency)
			writeString(buf, log.Idempotency.Key)
			// Store the log ID as value for quick lookup
			idValue := make([]byte, 8)
			binary.BigEndian.PutUint64(idValue, log.Id)
			if err := setOnBatch(batch, buf, idValue); err != nil {
				return fmt.Errorf("inserting idempotency index: %w", err)
			}
		}
	}

	if len(update.BalanceDiffs) > 0 {
		for ledger, ledgerBalanceDiff := range update.BalanceDiffs {
			for account, assets := range ledgerBalanceDiff {
				for asset, diff := range assets {
					writeLedgerPrefix(buf, ledger)
					writeByte(buf, keyPrefixBalanceDiff)
					writeString(buf, account)
					writeString(buf, asset)
					writeInt64(buf, time.Now().UnixNano())

					if err := setOnBatch(batch, buf, marshalBigInt(diff)); err != nil {
						return fmt.Errorf("storing balance diff for ledger %s account %s asset %s: %w", ledger, account, asset, err)
					}
				}
			}
		}
	}

	if len(update.AccountMetadata) > 0 {
		for ledger, ledgerAccountMetadata := range update.AccountMetadata {
			for account, metadataMap := range ledgerAccountMetadata {
				for metaKey, value := range metadataMap {
					writeLedgerPrefix(buf, ledger)
					writeByte(buf, keyPrefixAccountMetadata)
					writeString(buf, account)
					writeString(buf, metaKey)

					if err := setOnBatch(batch, buf, []byte(value)); err != nil {
						return fmt.Errorf("upserting account metadata: %w", err)
					}
				}
			}
		}
	}

	// Apply account metadata deletions
	if len(update.AccountMetadataDeletes) > 0 {
		for ledger, ledgerAccountMetadataDeletes := range update.AccountMetadataDeletes {
			for account, keys := range ledgerAccountMetadataDeletes {
				for _, metaKey := range keys {
					writeLedgerPrefix(buf, ledger)
					writeByte(buf, keyPrefixAccountMetadata)
					writeString(buf, account)
					writeString(buf, metaKey)

					if err := deleteOnBatch(batch, buf); err != nil {
						return fmt.Errorf("deleting account metadata key: %w", err)
					}
				}
			}
		}
	}

	// Apply transaction ID to log ID mappings
	if len(update.TransactionIDs) > 0 {
		for ledger, ledgerTransactionIDs := range update.TransactionIDs {
			for transactionID, logID := range ledgerTransactionIDs {
				writeLedgerPrefix(buf, ledger)
				writeByte(buf, keyPrefixTransactionID)
				writeUInt64(buf, transactionID)

				// Store log ID as 8-byte big-endian value
				logIDValue := make([]byte, 8)
				binary.BigEndian.PutUint64(logIDValue, logID)
				if err := setOnBatch(batch, buf, logIDValue); err != nil {
					return fmt.Errorf("inserting transaction ID mapping for ledger %s transaction %d: %w", ledger, transactionID, err)
				}
			}
		}
	}

	// Apply reverted transaction IDs
	if len(update.RevertedTransactionIDs) > 0 {
		for ledger, ledgerRevertedTransactions := range update.RevertedTransactionIDs {
			for transactionID := range ledgerRevertedTransactions {
				writeLedgerPrefix(buf, ledger)
				writeByte(buf, keyPrefixRevertedTxID)
				writeUInt64(buf, transactionID)

				// Store empty value (just presence indicates reverted)
				if err := setOnBatch(batch, buf, []byte{1}); err != nil {
					return fmt.Errorf("inserting reverted transaction ID for ledger %s transaction %d: %w", ledger, transactionID, err)
				}
			}
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Logs inserted into Pebble")
	return nil
}

// ============================================================================
// LogReader Implementation
// ============================================================================

// logCursor implements store.Cursor[*ledgerpb.Log] for Pebble.
type logCursor struct {
	iter *pebble.Iterator
	s    *RuntimeStore
}

func (c *logCursor) Next(ctx context.Context) (*ledgerpb.Log, error) {
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

func (c *logCursor) Close() error {
	if c.iter != nil {
		return c.iter.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger.
// Logs are returned in ascending order by id.
// from: optional log id to start from (0 = from beginning).
// to: optional log id to stop at (0 = until end, inclusive).
func (s *RuntimeStore) GetAllLogs(ctx context.Context, ledger string, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	// Set up iterator bounds

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixLog)
	if from > 0 {
		writeInt64(buf, int64(from))
	}
	lowerBound := buf.Bytes()

	buf = bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixLog)
	if to > 0 {
		writeInt64(buf, int64(to+1))
	} else {
		if _, err := buf.Write([]byte{
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		}); err != nil {
			return nil, err
		}
	}
	upperBound := buf.Bytes()

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

// GetLogByID retrieves a log by its ID for a specific ledger.
func (s *RuntimeStore) GetLogByID(ctx context.Context, ledger string, id uint64) (*ledgerpb.Log, error) {
	return s.GetLogWithID(ctx, ledger, id)
}

// GetLogWithID retrieves a log by its ID for a specific ledger.
func (s *RuntimeStore) GetLogWithID(ctx context.Context, ledger string, id uint64) (*ledgerpb.Log, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixLog)
	writeInt64(buf, int64(id))

	value, closer, err := s.db.Get(buf.Bytes())
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

// ============================================================================
// RuntimeStore Implementation
// ============================================================================

// GetBalances retrieves balances from Pebble for a specific ledger (implements store.RuntimeStore)
// Sums all balance diffs for each account/asset combination
func (s *RuntimeStore) GetBalances(ctx context.Context, ledger string, balanceQuery map[string][]string) (ledgerpb.Balances, error) {
	result := make(ledgerpb.Balances)

	buf := bytes.NewBuffer(nil)

	// Build query for each account/asset combination
	for account, assets := range balanceQuery {
		if len(assets) == 0 {
			continue
		}
		buf.Reset()

		// Initialize account map
		result[account] = make(map[string]*big.Int)

		// Query each asset
		for _, asset := range assets {
			// Initialize balance to zero
			balance := big.NewInt(0)

			// Iterate over all balance diffs for this ledger/account/asset combination
			writeLedgerPrefix(buf, ledger)
			writeByte(buf, keyPrefixBalanceDiff)
			writeString(buf, account)
			writeString(buf, asset)
			lowerBound := buf.Bytes()

			writeByte(buf, 0xFF)
			upperBound := buf.Bytes()

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lowerBound,
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

				diff := unmarshalBigInt(valueBytes)
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

// GetAccountMetadata retrieves account metadata for multiple accounts from Pebble for a specific ledger (implements store.RuntimeStore)
func (s *RuntimeStore) GetAccountMetadata(ctx context.Context, ledger string, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata)

	// Initialize with empty metadata for all requested accounts
	for _, account := range accounts {
		result[account] = make(metadata.Metadata)
	}

	buf := bytes.NewBuffer(nil)

	// Iterate over all accounts and read their metadata
	for _, account := range accounts {
		buf.Reset()

		writeLedgerPrefix(buf, ledger)
		writeByte(buf, keyPrefixAccountMetadata)
		writeString(buf, account)
		lowerBound := buf.Bytes()

		writeByte(buf, 0xFF)
		upperBound := buf.Bytes()

		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		})
		if err != nil {
			return nil, fmt.Errorf("creating iterator for ledger %s account %s: %w", ledger, account, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// Extract metadata key from full key (format: "met{ledger}:{account}:{key}")
			metadataKeyBytes := key[len(lowerBound):]
			metadataKey := string(metadataKeyBytes)
			valueBytes, err := iter.ValueAndErr()
			if err != nil {
				return nil, fmt.Errorf("reading metadata value for ledger %s account %s key %s: %w", ledger, account, metadataKey, err)
			}

			// Metadata values are stored as strings directly
			valueStr := string(valueBytes)
			result[account][metadataKey] = valueStr
		}
		_ = iter.Close()
	}

	return result, nil
}

// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key for a specific ledger (implements store.RuntimeStore)
func (s *RuntimeStore) GetLogIDForIdempotencyKey(ctx context.Context, ledger string, idempotencyKey string) (uint64, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixIdempotency)
	writeString(buf, idempotencyKey)

	value, closer, err := s.db.Get(buf.Bytes())
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

// GetLogIDForTransactionID retrieves the log ID for a given transaction ID for a specific ledger (implements store.RuntimeStore)
func (s *RuntimeStore) GetLogIDForTransactionID(ctx context.Context, ledger string, transactionID uint64) (uint64, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixTransactionID)
	writeUInt64(buf, transactionID)

	value, closer, err := s.db.Get(buf.Bytes())
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

// IsTransactionReverted checks if a transaction has been reverted for a specific ledger (implements store.RuntimeStore)
func (s *RuntimeStore) IsTransactionReverted(ctx context.Context, ledger string, transactionID uint64) (bool, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixRevertedTxID)
	writeUInt64(buf, transactionID)

	_, closer, err := s.db.Get(buf.Bytes())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("querying reverted transaction ID: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	return true, nil
}

// Metrics returns Pebble database metrics (implements MetricsAware)
func (s *RuntimeStore) Metrics() any {
	return s.db.Metrics()
}

func writeLedgerPrefix(buf *bytes.Buffer, ledger string) {
	if _, err := buf.WriteString(ledger); err != nil {
		panic(err)
	}
	if err := buf.WriteByte('/'); err != nil {
		panic(err)
	}
}

func writeUInt64(buf *bytes.Buffer, value uint64) {
	if err := binary.Write(buf, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(buf *bytes.Buffer, value int64) {
	if err := binary.Write(buf, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeString(buf *bytes.Buffer, value string) {
	if _, err := buf.WriteString(value); err != nil {
		panic(err)
	}
}

func writeByte(buf *bytes.Buffer, value byte) {
	if err := buf.WriteByte(value); err != nil {
		panic(err)
	}
}

func setOnBatch(batch *pebble.Batch, buf *bytes.Buffer, value []byte) error {
	if err := batch.Set(buf.Bytes(), value, pebble.NoSync); err != nil {
		return fmt.Errorf("inserting log: %w", err)
	}
	buf.Reset()

	return nil
}

func deleteOnBatch(batch *pebble.Batch, buf *bytes.Buffer) error {
	if err := batch.Delete(buf.Bytes(), pebble.NoSync); err != nil {
		return fmt.Errorf("inserting log: %w", err)
	}
	buf.Reset()

	return nil
}

func marshalBigInt(x *big.Int) []byte {
	if x == nil {
		return []byte{0} // convention: nil => 0
	}
	sign := byte(0)
	if x.Sign() < 0 {
		sign = 1
	}
	mag := new(big.Int).Abs(x).Bytes()
	out := make([]byte, 1+len(mag))
	out[0] = sign
	copy(out[1:], mag)
	return out
}

func unmarshalBigInt(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}
	sign := b[0]
	x := new(big.Int).SetBytes(b[1:])
	if sign == 1 && x.Sign() != 0 {
		x.Neg(x)
	}
	return x
}
