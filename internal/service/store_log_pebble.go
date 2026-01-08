package service

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// Pebble Log Store Implementation
// ============================================================================

// PebbleLogStore is a Pebble implementation of LogStore
type PebbleLogStore struct {
	db     *pebble.DB
	logger logging.Logger
}

// Key prefixes for Pebble storage
const (
	logKeyPrefix = "log:"
)

// NewPebbleLogStore creates a new PebbleLogStore instance
func NewPebbleLogStore(
	dataDir string,
	logger logging.Logger,
	meter metric.Meter,
) (*PebbleLogStore, error) {
	// Create data directory if it doesn't exist
	dbPath := filepath.Join(dataDir, "logs")

	opts := &pebble.Options{
		EventListener: NewPebbleMetricsListener(meter),
	}
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble database: %w", err)
	}

	store := &PebbleLogStore{
		db:     db,
		logger: logger,
	}

	return store, nil
}

// Close closes the Pebble database
func (s *PebbleLogStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// logKey returns the key for a log entry
func logKey(id uint64) []byte {
	// Use big-endian encoding for proper lexicographic ordering
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return append([]byte(logKeyPrefix), key...)
}

// ============================================================================
// LogWriter Implementation
// ============================================================================

// InsertLogs inserts logs into Pebble (implements LogWriter)
func (s *PebbleLogStore) InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	if len(logs) == 0 {
		return nil
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
		if err := batch.Set(key, logBinary, pebble.Sync); err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}

		// Also create an index by idempotency key if present
		if log.IdempotencyKey != "" {
			idempotencyKey := []byte(fmt.Sprintf("idempotency:%s", log.IdempotencyKey))
			// Store the log ID as value for quick lookup
			idValue := make([]byte, 8)
			binary.BigEndian.PutUint64(idValue, log.Id)
			if err := batch.Set(idempotencyKey, idValue, pebble.Sync); err != nil {
				return fmt.Errorf("inserting idempotency index: %w", err)
			}
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Logs inserted into Pebble")
	return nil
}

// ============================================================================
// LogReader Implementation
// ============================================================================

// pebbleLogCursor implements Cursor[*ledgerpb.Log] for Pebble
type pebbleLogCursor struct {
	iter  *pebble.Iterator
	store *PebbleLogStore
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

// GetAllLogs returns a cursor to iterate over all logs (implements LogReader)
// Logs are returned in ascending order by id
// from: optional log id to start from (0 = from beginning)
// to: optional log id to stop at (0 = until end, inclusive)
func (s *PebbleLogStore) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	// Set up iterator bounds
	lowerBound := []byte(logKeyPrefix)
	upperBound := append([]byte(logKeyPrefix), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

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

// GetLogWithID retrieves a log by its ID
func (s *PebbleLogStore) GetLogWithID(ctx context.Context, id uint64) (*ledgerpb.Log, error) {
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

// GetLogWithIdempotencyKey retrieves a log by its idempotency key
func (s *PebbleLogStore) GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledgerpb.Log, error) {
	if idempotencyKey == "" {
		return nil, nil
	}

	// Look up log ID from idempotency index
	idempotencyKeyBytes := []byte(fmt.Sprintf("idempotency:%s", idempotencyKey))
	idValue, closer, err := s.db.Get(idempotencyKeyBytes)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("getting log by idempotency key: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	// Extract log ID
	logID := binary.BigEndian.Uint64(idValue)

	// Get log by ID
	return s.GetLogWithID(ctx, logID)
}

// GetLastLog retrieves the last log by ID
func (s *PebbleLogStore) GetLastLog(ctx context.Context) (*ledgerpb.Log, error) {
	// Iterate backwards from the end
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(logKeyPrefix),
		UpperBound: append([]byte(logKeyPrefix), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF),
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	// Start from the last key
	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return nil, fmt.Errorf("getting last log: %w", err)
		}
		// No logs found
		return nil, nil
	}

	// Read protobuf Log
	value, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading log value: %w", err)
	}

	// Unmarshal protobuf Log
	log := &ledgerpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log from protobuf: %w", err)
	}

	return log, nil
}

// Metrics returns Pebble database metrics (implements MetricsAware)
func (s *PebbleLogStore) Metrics() any {
	return s.db.Metrics()
}
