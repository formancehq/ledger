package service

import (
	"github.com/formancehq/go-libs/v3/logging"

	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
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

// createTables creates the necessary tables for logs and balances
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

	// Create balances table
	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS balances (
			ledger VARCHAR(256) NOT NULL,
			account VARCHAR(256) NOT NULL,
			asset VARCHAR(256) NOT NULL,
			balance NUMERIC NOT NULL DEFAULT 0,
			PRIMARY KEY (ledger, account, asset)
		);
		
		CREATE INDEX IF NOT EXISTS idx_balances_ledger ON balances(ledger);
		CREATE INDEX IF NOT EXISTS idx_balances_account ON balances(ledger, account);
	`)
	if err != nil {
		return fmt.Errorf("creating balances table: %w", err)
	}

	// Create function to update balances for NEW_TRANSACTION
	_, err = s.db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION update_balances()
		RETURNS TRIGGER AS $$
		DECLARE
			posting JSONB;
		BEGIN
			-- Process each posting in the transaction
			FOR posting IN SELECT * FROM jsonb_array_elements(NEW.data->'transaction'->'postings')
			LOOP
				-- Update source account (subtract amount)
				INSERT INTO balances (ledger, account, asset, balance)
				VALUES (
					NEW.ledger,
					posting->>'source',
					posting->>'asset',
					-(posting->>'amount')::NUMERIC
				)
				ON CONFLICT (ledger, account, asset) DO UPDATE SET
					balance = balances.balance - (posting->>'amount')::NUMERIC;
				
				-- Update destination account (add amount)
				INSERT INTO balances (ledger, account, asset, balance)
				VALUES (
					NEW.ledger,
					posting->>'destination',
					posting->>'asset',
					(posting->>'amount')::NUMERIC
				)
				ON CONFLICT (ledger, account, asset) DO UPDATE SET
					balance = balances.balance + (posting->>'amount')::NUMERIC;
			END LOOP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`)
	if err != nil {
		return fmt.Errorf("creating update_balances_new_transaction function: %w", err)
	}

	// Create trigger for NEW_TRANSACTION
	_, err = s.db.ExecContext(ctx, `
		DROP TRIGGER IF EXISTS update_balances_on_transaction ON logs;
		CREATE TRIGGER update_balances_on_transaction
		AFTER INSERT ON logs
		FOR EACH ROW
		WHEN (NEW.type = 'NEW_TRANSACTION' OR NEW.type = 'REVERTED_TRANSACTION')
		EXECUTE FUNCTION update_balances();
	`)
	if err != nil {
		return fmt.Errorf("creating new transaction balances trigger: %w", err)
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

// GetBalances retrieves balances from the balances table (implements BalancesStore)
func (s *PostgresLogStore) GetBalances(ctx context.Context, ledgerName string, balanceQuery map[string][]string) (ledger.Balances, error) {
	result := make(ledger.Balances)

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

		// Build placeholders for IN clause (PostgreSQL uses $1, $2, etc.)
		placeholders := make([]string, len(assets))
		args := make([]interface{}, len(assets)+2)
		args[0] = ledgerName
		args[1] = account

		for i, asset := range assets {
			placeholders[i] = fmt.Sprintf("$%d", i+3)
			args[i+2] = asset
		}

		query := fmt.Sprintf(`
			SELECT asset, balance::text FROM balances
			WHERE ledger = $1 AND account = $2 AND asset IN (%s)
		`, strings.Join(placeholders, ", "))

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("querying balances: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var asset string
			var balanceStr string
			if err := rows.Scan(&asset, &balanceStr); err != nil {
				return nil, fmt.Errorf("scanning balance row: %w", err)
			}

			balance, ok := new(big.Int).SetString(balanceStr, 10)
			if !ok {
				return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
			}
			result[account][asset] = balance
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating balance rows: %w", err)
		}

		// Set zero balance for assets that don't exist in the database
		for _, asset := range assets {
			if _, exists := result[account][asset]; !exists {
				result[account][asset] = big.NewInt(0)
			}
		}
	}

	return result, nil
}
