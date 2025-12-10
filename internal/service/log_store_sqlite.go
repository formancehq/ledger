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
	_ "modernc.org/sqlite"
)

// SQLiteLogStore is a SQLite implementation of LogStore
type SQLiteLogStore struct {
	db     *sql.DB
	logger logging.Logger
}

// NewSQLiteLogStore creates a new SQLite log store
func NewSQLiteLogStore(ctx context.Context, dsn string, logger logging.Logger) (*SQLiteLogStore, error) {
	// Open SQLite database
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database: %w", err)
	}

	// Enable foreign keys and set pragmas
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return nil, fmt.Errorf("enabling foreign keys: %w", err)
	}

	store := &SQLiteLogStore{
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
func (s *SQLiteLogStore) createTables(ctx context.Context) error {
	// Create logs table
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS logs (
			id INTEGER,
			type TEXT NOT NULL,
			data TEXT NOT NULL,
			date TEXT,
			ledger TEXT NOT NULL,
			idempotency_key TEXT UNIQUE,
			idempotency_hash TEXT UNIQUE
		);
		
		CREATE INDEX IF NOT EXISTS idx_logs_idempotency_key ON logs(idempotency_key);
		CREATE INDEX IF NOT EXISTS idx_logs_id ON logs(id);
		CREATE INDEX IF NOT EXISTS idx_logs_ledger ON logs(ledger);
	`)

	// Add ledger column if it doesn't exist (for migration)
	_, _ = s.db.ExecContext(ctx, `ALTER TABLE logs ADD COLUMN ledger TEXT;`)
	if err != nil {
		return fmt.Errorf("creating logs table: %w", err)
	}

	return nil
}

// Close closes the database connection
func (s *SQLiteLogStore) Close() error {
	return s.db.Close()
}

// InsertLogs inserts logs into the SQLite database (implements LogWriter)
func (s *SQLiteLogStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
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
		VALUES (?, ?, ?, ?, ?, ?, ?)
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

		// Format date as RFC3339 string (SQLite stores dates as TEXT)
		var dateStr sql.NullString
		if !log.Date.IsZero() {
			dateStr = sql.NullString{
				String: log.Date.Format(stdtime.RFC3339),
				Valid:  true,
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
			dateStr,
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

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Logs inserted into SQLite")
	return nil
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key (implements LogReader)
func (s *SQLiteLogStore) GetLogWithIdempotencyKey(ctx context.Context, ledgerName string, idempotencyKey string) (*ledger.Log, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = ? AND idempotency_key = ?
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
func (s *SQLiteLogStore) GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = ?
		ORDER BY id DESC
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
func (s *SQLiteLogStore) scanLog(row *sql.Row) (*ledger.Log, error) {
	var id sql.NullInt64
	var logType string
	var dataJSON string
	var dateStr sql.NullString
	var ledgerName string
	var idempotencyKey sql.NullString
	var idempotencyHash sql.NullString

	err := row.Scan(&id, &logType, &dataJSON, &dateStr, &ledgerName, &idempotencyKey, &idempotencyHash)
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
	if dateStr.Valid {
		date, err := stdtime.Parse(stdtime.RFC3339, dateStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing date: %w", err)
		}
		log.Date = libtime.New(date)
	}

	// Set idempotency fields
	if idempotencyKey.Valid {
		log.IdempotencyKey = idempotencyKey.String
	}
	if idempotencyHash.Valid {
		log.IdempotencyHash = idempotencyHash.String
	}

	return log, nil
}

// sqliteLogCursor implements Cursor[ledger.Log] for SQLite
type sqliteLogCursor struct {
	rows  *sql.Rows
	store *SQLiteLogStore
}

func (c *sqliteLogCursor) Next(ctx context.Context) (ledger.Log, error) {
	if !c.rows.Next() {
		if err := c.rows.Err(); err != nil {
			return ledger.Log{}, err
		}
		return ledger.Log{}, io.EOF
	}

	var id sql.NullInt64
	var logType string
	var dataJSON string
	var dateStr sql.NullString
	var ledgerName string
	var idempotencyKey sql.NullString
	var idempotencyHash sql.NullString

	err := c.rows.Scan(&id, &logType, &dataJSON, &dateStr, &ledgerName, &idempotencyKey, &idempotencyHash)
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
	if dateStr.Valid {
		date, err := stdtime.Parse(stdtime.RFC3339, dateStr.String)
		if err != nil {
			return ledger.Log{}, fmt.Errorf("parsing date: %w", err)
		}
		log.Date = libtime.New(date)
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

func (c *sqliteLogCursor) Close() error {
	if c.rows != nil {
		return c.rows.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger (implements LogReader)
// Logs are returned in descending order by ID
func (s *SQLiteLogStore) GetAllLogs(ctx context.Context, ledgerName string) (*Cursor[ledger.Log], error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs
		WHERE ledger = ?
		ORDER BY id DESC
	`, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("querying logs: %w", err)
	}

	cursor := &sqliteLogCursor{
		rows:  rows,
		store: s,
	}

	var cursorInterface Cursor[ledger.Log] = cursor
	return &cursorInterface, nil
}
