package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	stdtime "time"

	_ "modernc.org/sqlite" // SQLite driver

	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// SQLiteLogStore is a SQLite implementation of LogStore
type SQLiteLogStore struct {
	db   *sql.DB
	mu   sync.RWMutex
	path string
}

// NewSQLiteLogStore creates a new SQLite-based LogStore
func NewSQLiteLogStore(dbPath string) (*SQLiteLogStore, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	store := &SQLiteLogStore{
		db:   db,
		path: dbPath,
	}

	// Create the logs table if it doesn't exist
	if err := store.createTable(); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating table: %w", err)
	}

	return store, nil
}

// createTable creates the logs table if it doesn't exist
func (s *SQLiteLogStore) createTable() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS logs (
		id INTEGER PRIMARY KEY,
		type TEXT NOT NULL,
		data TEXT NOT NULL,
		date TEXT NOT NULL,
		idempotency_key TEXT UNIQUE,
		idempotency_hash TEXT UNIQUE,
		hash BLOB NOT NULL
	);
	
	CREATE INDEX IF NOT EXISTS idx_logs_idempotency_key ON logs(idempotency_key);
	CREATE INDEX IF NOT EXISTS idx_logs_id ON logs(id);
	`

	_, err := s.db.Exec(createTableSQL)
	return err
}

// Close closes the database connection
func (s *SQLiteLogStore) Close() error {
	return s.db.Close()
}

// InsertLogs inserts logs into the SQLite database
func (s *SQLiteLogStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO logs (id, type, data, date, idempotency_key, idempotency_hash, hash)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, log := range logs {
		// Serialize the log data to JSON
		dataJSON, err := json.Marshal(log.Data)
		if err != nil {
			return fmt.Errorf("marshaling log data: %w", err)
		}

		// Convert date to string (RFC3339 format)
		dateStr := ""
		if !log.Date.IsZero() {
			dateStr = log.Date.Time.Format(stdtime.RFC3339)
		}

		// Get ID value
		var id interface{}
		if log.ID != nil {
			id = *log.ID
		} else {
			id = nil
		}

		// Get idempotency key (can be empty string)
		idempotencyKey := log.IdempotencyKey
		if idempotencyKey == "" {
			idempotencyKey = ""
		}

		// Get idempotency hash (can be empty string)
		idempotencyHash := log.IdempotencyHash
		if idempotencyHash == "" {
			idempotencyHash = ""
		}

		_, err = stmt.ExecContext(ctx,
			id,
			log.Type.String(),
			string(dataJSON),
			dateStr,
			idempotencyKey,
			idempotencyHash,
			log.Hash,
		)
		if err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key
func (s *SQLiteLogStore) GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledger.Log, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, idempotency_key, idempotency_hash, hash
		FROM logs
		WHERE idempotency_key = ?
	`, idempotencyKey)

	return s.scanLog(row)
}

// GetLastLog retrieves the last log (highest ID)
func (s *SQLiteLogStore) GetLastLog(ctx context.Context) (*ledger.Log, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, data, date, idempotency_key, idempotency_hash, hash
		FROM logs
		ORDER BY id DESC
		LIMIT 1
	`)

	return s.scanLog(row)
}

// GetAllLogs retrieves all logs ordered by ID
func (s *SQLiteLogStore) GetAllLogs(ctx context.Context) ([]ledger.Log, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, type, data, date, idempotency_key, idempotency_hash, hash
		FROM logs
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("querying logs: %w", err)
	}
	defer rows.Close()

	var logs []ledger.Log
	for rows.Next() {
		log, err := s.scanLog(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning log: %w", err)
		}
		if log != nil {
			logs = append(logs, *log)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return logs, nil
}

// scanLog scans a row into a Log struct
func (s *SQLiteLogStore) scanLog(scanner interface {
	Scan(dest ...interface{}) error
}) (*ledger.Log, error) {
	var id sql.NullInt64
	var logType string
	var dataJSON string
	var dateStr sql.NullString
	var idempotencyKey sql.NullString
	var idempotencyHash sql.NullString
	var hash []byte

	err := scanner.Scan(&id, &logType, &dataJSON, &dateStr, &idempotencyKey, &idempotencyHash, &hash)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	// Parse log type
	logTypeEnum := ledger.LogTypeFromString(logType)

	// Parse log data
	var data ledger.LogPayload
	data, err = ledger.HydrateLog(logTypeEnum, []byte(dataJSON))
	if err != nil {
		return nil, fmt.Errorf("hydrating log data: %w", err)
	}

	// Parse date
	var date time.Time
	if dateStr.Valid && dateStr.String != "" {
		parsed, err := stdtime.Parse(stdtime.RFC3339, dateStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing date: %w", err)
		}
		date = time.New(parsed)
	}

	// Build log
	log := ledger.Log{
		Type: logTypeEnum,
		Data: data,
		Date: date,
		Hash: hash,
	}

	// Set ID if present
	if id.Valid {
		idVal := uint64(id.Int64)
		log.ID = &idVal
	}

	// Set idempotency key if present
	if idempotencyKey.Valid {
		log.IdempotencyKey = idempotencyKey.String
	}

	// Set idempotency hash if present
	if idempotencyHash.Valid {
		log.IdempotencyHash = idempotencyHash.String
	}

	return &log, nil
}
