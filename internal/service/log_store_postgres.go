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
	_ "github.com/lib/pq"
)

// PostgresConfig represents the configuration for a PostgreSQL bucket driver
type PostgresConfig struct {
	DSN string `json:"dsn"` // Data Source Name (connection string)
}

// PostgresLogStore is a PostgreSQL implementation of LogStore
type PostgresLogStore struct {
	db     *sql.DB
	logger logging.Logger
}

// NewPostgresLogStore creates a new PostgreSQL log store
func NewPostgresLogStore(ctx context.Context, dsn string, logger logging.Logger) (*PostgresLogStore, error) {
	// Open PostgreSQL database
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening postgres database: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging postgres database: %w", err)
	}

	store := &PostgresLogStore{
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
func (s *PostgresLogStore) createTables(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		CREATE TYPE log_type AS ENUM ('SET_METADATA', 'NEW_TRANSACTION', 'REVERTED_TRANSACTION', 'DELETE_METADATA');
		CREATE TABLE IF NOT EXISTS logs (
			id BIGINT,
			type log_type NOT NULL,
			data JSONB NOT NULL,
			date TIMESTAMPTZ,
			ledger VARCHAR(256) NOT NULL,
			idempotency_key VARCHAR(256),
			idempotency_hash VARCHAR(256),
			CONSTRAINT logs_idempotency_key_unique UNIQUE (idempotency_key),
			PRIMARY KEY (ledger, id)
		);
		
		CREATE INDEX IF NOT EXISTS idx_logs_idempotency_key ON logs(idempotency_key) WHERE idempotency_key IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_logs_id ON logs(id) WHERE id IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_logs_ledger_id ON logs(ledger, id);
	`)
	if err != nil {
		return fmt.Errorf("creating logs table: %w", err)
	}

	return nil
}

// Close closes the database connection
func (s *PostgresLogStore) Close() error {
	return s.db.Close()
}

// InsertLogs inserts logs into the PostgreSQL database (implements LogWriter)
func (s *PostgresLogStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Use a transaction for batch insert
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO logs (type, data, date, ledger, idempotency_key, idempotency_hash, id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	for _, log := range logs {
		// Marshal data to JSON
		dataJSON, err := json.Marshal(log.Data)
		if err != nil {
			return fmt.Errorf("marshaling log data: %w", err)
		}

		// Format date as TIMESTAMPTZ (PostgreSQL handles timezone-aware timestamps)
		var datePtr *stdtime.Time
		if !log.Date.IsZero() {
			datePtr = &log.Date.Time
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

		var id sql.NullInt64
		if log.ID != nil {
			id = sql.NullInt64{
				Int64: int64(*log.ID),
				Valid: true,
			}
		}

		_, err = stmt.ExecContext(ctx,
			log.Type.String(),
			string(dataJSON),
			datePtr,
			log.Ledger,
			idempotencyKey,
			idempotencyHash,
			id,
		)
		if err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Logs inserted into PostgreSQL")
	return nil
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key (implements LogReader)
func (s *PostgresLogStore) GetLogWithIdempotencyKey(ctx context.Context, ledgerName string, idempotencyKey string) (*ledger.Log, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = $1 AND idempotency_key = $2
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
func (s *PostgresLogStore) GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = $1
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
func (s *PostgresLogStore) scanLog(row *sql.Row) (*ledger.Log, error) {
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

// postgresLogCursor implements Cursor[ledger.Log] for PostgreSQL
type postgresLogCursor struct {
	rows  *sql.Rows
	store *PostgresLogStore
}

func (c *postgresLogCursor) Next(ctx context.Context) (ledger.Log, error) {
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

func (c *postgresLogCursor) Close() error {
	if c.rows != nil {
		return c.rows.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger (implements LogReader)
// Logs are returned in ascending order by ID
func (s *PostgresLogStore) GetAllLogs(ctx context.Context, ledgerName string) (*Cursor[ledger.Log], error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = $1
		ORDER BY id ASC NULLS FIRST
	`, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("querying logs: %w", err)
	}

	cursor := &postgresLogCursor{
		rows:  rows,
		store: s,
	}

	var cursorInterface Cursor[ledger.Log] = cursor
	return &cursorInterface, nil
}
