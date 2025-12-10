package service

import (
	"github.com/formancehq/go-libs/v3/logging"

	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	stdtime "time"

	libtime "github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouseLogStore is a ClickHouse implementation of LogStore
type ClickHouseLogStore struct {
	db     *sql.DB
	logger logging.Logger
}

// NewClickHouseLogStore creates a new ClickHouse log store
func NewClickHouseLogStore(ctx context.Context, dsn string, logger logging.Logger) (*ClickHouseLogStore, error) {
	// Open ClickHouse database
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse database: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging clickhouse database: %w", err)
	}

	store := &ClickHouseLogStore{
		db:     db,
		logger: logger,
	}

	// Create tables if they don't exist
	if err := store.createTables(ctx); err != nil {
		return nil, fmt.Errorf("creating tables: %w", err)
	}

	return store, nil
}

// createTables creates the necessary tables for logs
func (s *ClickHouseLogStore) createTables(ctx context.Context) error {
	// Create logs table with MergeTree engine
	// ClickHouse doesn't support UNIQUE constraints in the same way as PostgreSQL,
	// so we'll use a ReplacingMergeTree to handle duplicates based on idempotency_key
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS logs (
			id Nullable(UInt64),
			type String NOT NULL,
			data String NOT NULL,
			date Nullable(DateTime64(9)),
			ledger String NOT NULL,
			idempotency_key Nullable(String),
			idempotency_hash Nullable(String)
		) ENGINE = MergeTree()
		ORDER BY (ledger, id)
		PARTITION BY toYYYYMM(date)
		SETTINGS index_granularity = 8192;
	`)
	if err != nil {
		return fmt.Errorf("creating logs table: %w", err)
	}

	// Create a materialized view or additional table for idempotency key lookups
	// Since ClickHouse doesn't support UNIQUE constraints, we'll create a separate
	// table for idempotency tracking using ReplacingMergeTree
	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS logs_idempotency (
			ledger String,
			idempotency_key String,
			id UInt64,
			type String,
			data String,
			date Nullable(DateTime64(9)),
			idempotency_hash Nullable(String),
			_updated DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(_updated)
		ORDER BY (ledger, idempotency_key)
		SETTINGS index_granularity = 8192;
	`)
	if err != nil {
		return fmt.Errorf("creating logs_idempotency table: %w", err)
	}

	return nil
}

// Close closes the database connection
func (s *ClickHouseLogStore) Close() error {
	return s.db.Close()
}

// InsertLogs inserts logs into the ClickHouse database (implements LogWriter)
func (s *ClickHouseLogStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// ClickHouse doesn't support transactions in the same way as PostgreSQL,
	// but we can use batch inserts for better performance
	stmt, err := s.db.PrepareContext(ctx, `
		INSERT INTO logs (id, type, data, date, ledger, idempotency_key, idempotency_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	// Also prepare statement for idempotency table
	idempotencyStmt, err := s.db.PrepareContext(ctx, `
		INSERT INTO logs_idempotency (ledger, idempotency_key, id, type, data, date, idempotency_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing idempotency insert statement: %w", err)
	}
	defer func() {
		_ = idempotencyStmt.Close()
	}()

	for _, log := range logs {
		// Marshal data to JSON
		dataJSON, err := json.Marshal(log.Data)
		if err != nil {
			return fmt.Errorf("marshaling log data: %w", err)
		}

		// Format date as DateTime64 (ClickHouse handles nanoseconds)
		var datePtr *stdtime.Time
		if !log.Date.IsZero() {
			datePtr = &log.Date.Time
		}

		var id sql.NullInt64
		if log.ID != nil {
			id = sql.NullInt64{
				Int64: int64(*log.ID),
				Valid: true,
			}
		}

		var idempotencyKey sql.NullString
		if log.IdempotencyKey != "" {
			idempotencyKey = sql.NullString{
				String: log.IdempotencyKey,
				Valid:  true,
			}
		}

		var idempotencyHash sql.NullString
		if log.IdempotencyHash != "" {
			idempotencyHash = sql.NullString{
				String: log.IdempotencyHash,
				Valid:  true,
			}
		}

		// Insert into main logs table
		_, err = stmt.ExecContext(ctx,
			id,
			log.Type.String(),
			string(dataJSON),
			datePtr,
			log.Ledger,
			idempotencyKey,
			idempotencyHash,
		)
		if err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}

		// Insert into idempotency table if idempotency key exists
		if idempotencyKey.Valid {
			_, err = idempotencyStmt.ExecContext(ctx,
				log.Ledger,
				idempotencyKey.String,
				id,
				log.Type.String(),
				string(dataJSON),
				datePtr,
				idempotencyHash,
			)
			if err != nil {
				return fmt.Errorf("inserting log idempotency: %w", err)
			}
		}
	}

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Logs inserted into ClickHouse")
	return nil
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key (implements LogReader)
func (s *ClickHouseLogStore) GetLogWithIdempotencyKey(ctx context.Context, ledgerName string, idempotencyKey string) (*ledger.Log, error) {
	// Use FINAL to get the latest version from ReplacingMergeTree
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs_idempotency FINAL
		WHERE ledger = ? AND idempotency_key = ?
		LIMIT 1
	`, ledgerName, idempotencyKey)

	log, err := s.scanLog(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("getting log by idempotency key: %w", err)
	}
	return log, nil
}

// GetLastLog retrieves the last log by ID for a specific ledger (implements LogReader)
func (s *ClickHouseLogStore) GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = ?
		ORDER BY id DESC NULLS LAST
		LIMIT 1
	`, ledgerName)

	log, err := s.scanLog(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("getting last log: %w", err)
	}
	return log, nil
}

// scanLog scans a row into a Log struct
func (s *ClickHouseLogStore) scanLog(row *sql.Row) (*ledger.Log, error) {
	var id sql.NullInt64
	var logType string
	var dataJSON string
	var datePtr *stdtime.Time
	var ledgerName string
	var idempotencyKey sql.NullString
	var idempotencyHash sql.NullString

	err := row.Scan(&id, &logType, &dataJSON, &datePtr, &ledgerName, &idempotencyKey, &idempotencyHash)
	if err != nil {
		return nil, err
	}

	log := &ledger.Log{}

	// Set ID
	if id.Valid {
		idVal := uint64(id.Int64)
		log.ID = &idVal
	}

	// Set type
	log.Type = ledger.LogTypeFromString(logType)

	// Unmarshal data using HydrateLog
	log.Data, err = ledger.HydrateLog(log.Type, []byte(dataJSON))
	if err != nil {
		return nil, fmt.Errorf("hydrating log data: %w", err)
	}

	// Set date
	if datePtr != nil {
		log.Date = libtime.New(*datePtr)
	}

	// Set ledger
	log.Ledger = ledgerName

	// Set idempotency fields
	if idempotencyKey.Valid {
		log.IdempotencyKey = idempotencyKey.String
	}
	if idempotencyHash.Valid {
		log.IdempotencyHash = idempotencyHash.String
	}

	return log, nil
}

// clickHouseLogCursor implements Cursor[ledger.Log] for ClickHouse
type clickHouseLogCursor struct {
	rows  *sql.Rows
	store *ClickHouseLogStore
}

func (c *clickHouseLogCursor) Next(ctx context.Context) (ledger.Log, error) {
	if !c.rows.Next() {
		if err := c.rows.Err(); err != nil {
			return ledger.Log{}, err
		}
		return ledger.Log{}, io.EOF
	}

	var id sql.NullInt64
	var logType string
	var dataJSON string
	var datePtr *stdtime.Time
	var ledgerName string
	var idempotencyKey sql.NullString
	var idempotencyHash sql.NullString

	err := c.rows.Scan(&id, &logType, &dataJSON, &datePtr, &ledgerName, &idempotencyKey, &idempotencyHash)
	if err != nil {
		return ledger.Log{}, fmt.Errorf("scanning log row: %w", err)
	}

	log := ledger.Log{}

	// Set ID
	if id.Valid {
		idVal := uint64(id.Int64)
		log.ID = &idVal
	}

	// Set type
	log.Type = ledger.LogTypeFromString(logType)

	// Unmarshal data using HydrateLog
	log.Data, err = ledger.HydrateLog(log.Type, []byte(dataJSON))
	if err != nil {
		return ledger.Log{}, fmt.Errorf("hydrating log data: %w", err)
	}

	// Set date
	if datePtr != nil {
		log.Date = libtime.New(*datePtr)
	}

	// Set ledger
	log.Ledger = ledgerName

	// Set idempotency fields
	if idempotencyKey.Valid {
		log.IdempotencyKey = idempotencyKey.String
	}
	if idempotencyHash.Valid {
		log.IdempotencyHash = idempotencyHash.String
	}

	return log, nil
}

func (c *clickHouseLogCursor) Close() error {
	if c.rows != nil {
		return c.rows.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger (implements LogReader)
// Logs are returned in descending order by ID
func (s *ClickHouseLogStore) GetAllLogs(ctx context.Context, ledgerName string) (*Cursor[ledger.Log], error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = ?
		ORDER BY id DESC NULLS LAST
	`, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("querying logs: %w", err)
	}

	cursor := &clickHouseLogCursor{
		rows:  rows,
		store: s,
	}

	var cursorInterface Cursor[ledger.Log] = cursor
	return &cursorInterface, nil
}

