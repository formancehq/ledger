package service

import (
	"errors"

	"github.com/formancehq/go-libs/v3/logging"

	"context"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"math/big"
	"strings"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// ============================================================================
// SQLite Runtime Store Implementation
// ============================================================================

// SQLiteRuntimeStore is a SQLite implementation of RuntimeStore
// It stores balances and account metadata
type SQLiteRuntimeStore struct {
	db     *SQLDB
	logger logging.Logger

	// Prepared statements
	stmtGetIdempotency           *sql.Stmt
	stmtInsertBalance            *sql.Stmt
	stmtUpsertAccountMetadata    *sql.Stmt
	stmtDeleteAccountMetadata    *sql.Stmt
	stmtInsertIdempotency        *sql.Stmt
	stmtGetLastProcessedLogID    *sql.Stmt
	stmtUpdateLastProcessedLogID *sql.Stmt
}

// NewSQLiteRuntimeStore creates a new SQLiteRuntimeStore instance
func NewSQLiteRuntimeStore(db *SQLDB, logger logging.Logger) (*SQLiteRuntimeStore, error) {
	store := &SQLiteRuntimeStore{
		db:     db,
		logger: logger,
	}

	// Create tables first (required before preparing statements)
	ctx := context.Background()
	if err := store.createRuntimeTables(ctx); err != nil {
		return nil, fmt.Errorf("creating tables: %w", err)
	}

	// Prepare all statements
	var err error
	store.stmtGetIdempotency, err = db.PrepareContext(ctx, `
		SELECT hash, log_id
		FROM idempotency
		WHERE key = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("preparing getIdempotency statement: %w", err)
	}

	// todo: update will not work once the max precision will be reached
	store.stmtInsertBalance, err = db.PrepareContext(ctx, `
		INSERT INTO balances (account, asset, balance)
		VALUES (?, ?, ?)
		ON CONFLICT (account, asset) DO UPDATE
		SET balance = balance + excluded.balance
	`)
	if err != nil {
		return nil, fmt.Errorf("preparing insertBalance statement: %w", err)
	}

	store.stmtUpsertAccountMetadata, err = db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO account_metadata (account_address, key, value)
		VALUES (?, ?, ?) 
	`)
	if err != nil {
		return nil, fmt.Errorf("preparing upsertAccountMetadata statement: %w", err)
	}

	store.stmtDeleteAccountMetadata, err = db.PrepareContext(ctx, `
		DELETE FROM account_metadata
		WHERE account_address = ? AND key = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("preparing deleteAccountMetadata statement: %w", err)
	}

	store.stmtInsertIdempotency, err = db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO idempotency (key, hash, log_id)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("preparing insertIdempotency statement: %w", err)
	}

	store.stmtGetLastProcessedLogID, err = db.PrepareContext(ctx, `
		SELECT value
		FROM infos
		WHERE key = 'last_processed_log_id'
	`)
	if err != nil {
		return nil, fmt.Errorf("preparing getLastProcessedLogID statement: %w", err)
	}

	store.stmtUpdateLastProcessedLogID, err = db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO infos (key, value)
		VALUES ('last_processed_log_id', ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("preparing updateLastProcessedLogID statement: %w", err)
	}

	return store, nil
}

// createRuntimeTables creates the necessary tables for balances and account metadata
func (s *SQLiteRuntimeStore) createRuntimeTables(ctx context.Context) error {
	// Create balances table
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS balances (
			id INTEGER PRIMARY KEY,
			account TEXT NOT NULL,
			asset TEXT NOT NULL,
			balance TEXT NOT NULL DEFAULT '0',
			UNIQUE (asset, account)
		);
	`)
	if err != nil {
		return fmt.Errorf("creating balances table: %w", err)
	}

	// Create account_metadata table
	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS account_metadata (
			account_address TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (account_address, key)
		);
	`)
	if err != nil {
		return fmt.Errorf("creating account_metadata table: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS idempotency (
			key TEXT NOT NULL,
			hash BYTEA NOT NULL,
			log_id INTEGER NOT NULL,
			PRIMARY KEY (key)
		);
	`)
	if err != nil {
		return fmt.Errorf("creating idempotency table: %w", err)
	}

	// Create infos table
	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS infos (
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (key)
		);
	`)
	if err != nil {
		return fmt.Errorf("creating infos table: %w", err)
	}

	return nil
}

// ============================================================================
// RuntimeStore Update Implementation
// ============================================================================

// Update applies all runtime updates atomically
func (s *SQLiteRuntimeStore) Update(ctx context.Context, update RuntimeUpdate) error {
	// Use a transaction for atomic updates
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Apply balance differences
	if len(update.BalanceDiffs) > 0 {
		stmt := tx.StmtContext(ctx, s.stmtInsertBalance)
		defer func() {
			_ = stmt.Close()
		}()

		for account, assets := range update.BalanceDiffs {
			for asset, diff := range assets {
				if _, err := stmt.ExecContext(ctx, account, asset, diff.String()); err != nil {
					return fmt.Errorf("updating balance for account %s asset %s: %w", account, asset, err)
				}
			}
		}
	}

	// Apply account metadata updates
	if len(update.AccountMetadata) > 0 {
		stmt := tx.StmtContext(ctx, s.stmtUpsertAccountMetadata)
		defer func() {
			_ = stmt.Close()
		}()

		for accountAddr, metadataMap := range update.AccountMetadata {
			for key, value := range metadataMap {
				valueJSON, err := json.Marshal(value)
				if err != nil {
					return fmt.Errorf("marshaling metadata value: %w", err)
				}
				if _, err := stmt.ExecContext(ctx, accountAddr, key, string(valueJSON)); err != nil {
					return fmt.Errorf("upserting account metadata: %w", err)
				}
			}
		}
	}

	// Apply account metadata deletions
	if len(update.AccountMetadataDeletes) > 0 {
		stmt := tx.StmtContext(ctx, s.stmtDeleteAccountMetadata)
		defer func() {
			_ = stmt.Close()
		}()

		for accountAddr, keys := range update.AccountMetadataDeletes {
			for _, key := range keys {
				if _, err := stmt.ExecContext(ctx, accountAddr, key); err != nil {
					return fmt.Errorf("deleting account metadata key: %w", err)
				}
			}
		}
	}

	// Apply idempotency entries
	if len(update.IdempotencyKeys) > 0 {
		stmt := tx.StmtContext(ctx, s.stmtInsertIdempotency)
		defer func() {
			_ = stmt.Close()
		}()

		for key, entry := range update.IdempotencyKeys {
			if _, err := stmt.ExecContext(ctx, key, entry.Hash, entry.LogId); err != nil {
				return fmt.Errorf("inserting idempotency entry for key %s: %w", key, err)
			}
		}
	}

	// Update last processed log ID
	if update.LastProcessedLogID > 0 {
		stmt := tx.StmtContext(ctx, s.stmtUpdateLastProcessedLogID)
		defer func() {
			_ = stmt.Close()
		}()

		valueStr := fmt.Sprintf("%d", update.LastProcessedLogID)
		if _, err := stmt.ExecContext(ctx, valueStr); err != nil {
			return fmt.Errorf("updating last processed log ID: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// Close closes the database connection and prepared statements
func (s *SQLiteRuntimeStore) Close() error {
	var errs []error
	if s.stmtGetIdempotency != nil {
		if err := s.stmtGetIdempotency.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.stmtInsertBalance != nil {
		if err := s.stmtInsertBalance.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.stmtUpsertAccountMetadata != nil {
		if err := s.stmtUpsertAccountMetadata.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.stmtDeleteAccountMetadata != nil {
		if err := s.stmtDeleteAccountMetadata.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.stmtInsertIdempotency != nil {
		if err := s.stmtInsertIdempotency.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.stmtGetLastProcessedLogID != nil {
		if err := s.stmtGetLastProcessedLogID.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.stmtUpdateLastProcessedLogID != nil {
		if err := s.stmtUpdateLastProcessedLogID.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.db.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing store: %v", errs)
	}
	return nil
}

// ============================================================================
// RuntimeStore Implementation
// ============================================================================

// GetBalances retrieves balances from the balances table (implements RuntimeStore)
func (s *SQLiteRuntimeStore) GetBalances(ctx context.Context, balanceQuery map[string][]string) (ledgerpb.Balances, error) {
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

		// Build placeholders for IN clause
		placeholders := make([]string, len(assets))
		args := make([]interface{}, len(assets)+1)
		args[0] = account

		for i, asset := range assets {
			placeholders[i] = "?"
			args[i+1] = asset
		}

		query := fmt.Sprintf(`
			SELECT asset, balance FROM balances
			WHERE account = ? AND asset IN (%s)
		`, strings.Join(placeholders, ", "))

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("querying balances: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

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

// GetAccountMetadata retrieves account metadata for multiple accounts from account_metadata table (implements RuntimeStore)
func (s *SQLiteRuntimeStore) GetAccountMetadata(ctx context.Context, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata)

	// If no accounts requested, return empty map
	if len(accounts) == 0 {
		return result, nil
	}

	// Initialize with empty metadata for all requested accounts
	for _, account := range accounts {
		result[account] = make(metadata.Metadata)
	}

	// Build placeholders for IN clause
	placeholders := make([]string, len(accounts))
	args := make([]interface{}, len(accounts))
	for i, account := range accounts {
		placeholders[i] = "?"
		args[i] = account
	}

	query := fmt.Sprintf(`
		SELECT account_address, key, value
		FROM account_metadata
		WHERE account_address IN (%s)
	`, strings.Join(placeholders, ", "))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying account metadata: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var address string
		var key string
		var valueJSON string

		if err := rows.Scan(&address, &key, &valueJSON); err != nil {
			return nil, fmt.Errorf("scanning account metadata row: %w", err)
		}

		// Parse value JSON
		var value interface{}
		if err := json.Unmarshal([]byte(valueJSON), &value); err != nil {
			return nil, fmt.Errorf("unmarshaling metadata value for account %s key %s: %w", address, key, err)
		}

		// Ensure the account exists in result map
		if _, exists := result[address]; !exists {
			result[address] = make(metadata.Metadata)
		}
		// Convert value to string if it's a string, otherwise convert to string via JSON
		var valueStr string
		if strVal, ok := value.(string); ok {
			valueStr = strVal
		} else {
			valueJSON, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("marshaling metadata value for account %s key %s: %w", address, key, err)
			}
			valueStr = string(valueJSON)
		}
		result[address][key] = valueStr
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating account metadata rows: %w", err)
	}

	return result, nil
}

// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key (implements RuntimeStore)
func (s *SQLiteRuntimeStore) GetLogForIdempotencyKey(ctx context.Context, idempotencyKey string) ([]byte, uint64, error) {
	if idempotencyKey == "" {
		return nil, 0, nil
	}

	var hash []byte
	var logID uint64

	err := s.stmtGetIdempotency.QueryRowContext(ctx, idempotencyKey).Scan(&hash, &logID)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("querying idempotency entry: %w", err)
	}

	return hash, logID, nil
}

// GetLastProcessedLogID retrieves the ID of the last processed log from the infos table
func (s *SQLiteRuntimeStore) GetLastProcessedLogID(ctx context.Context) (uint64, error) {
	var valueStr string
	err := s.stmtGetLastProcessedLogID.QueryRowContext(ctx).Scan(&valueStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("querying last processed log ID: %w", err)
	}

	// Parse the value as uint64
	var lastLogID uint64
	if _, err := fmt.Sscanf(valueStr, "%d", &lastLogID); err != nil {
		return 0, fmt.Errorf("parsing last processed log ID: %w", err)
	}

	return lastLogID, nil
}

// Metrics returns SQLite database metrics (implements MetricsAware)
func (s *SQLiteRuntimeStore) Metrics() any {
	return getSQLiteMetrics(s.db)
}
