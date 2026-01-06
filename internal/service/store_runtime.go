package service

import (
	"github.com/formancehq/go-libs/v3/logging"

	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// ============================================================================
// SQLite Runtime Store Implementation
// ============================================================================

// SQLiteRuntimeStore is a SQLite implementation of RuntimeStore
// It stores balances and account metadata, and implements LogWriter
// to update these values when logs are written (but does not store the logs themselves)
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

// idempotencyEntry represents an idempotency key entry
type idempotencyEntry struct {
	key   string
	hash  string
	logID uint64
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
			hash TEXT NOT NULL,
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
// LogWriter Implementation (for updating balances and metadata)
// ============================================================================

// InsertLogs updates balances and account metadata based on the logs (implements LogWriter)
// This method does NOT store the logs themselves, only updates runtime data
func (s *SQLiteRuntimeStore) InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Use a transaction for batch updates
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Accumulate balance differences for all logs
	balanceDiffs := make(map[string]map[string]*big.Int)

	// Accumulate metadata operations for batch processing
	accountMetadataBatch := make(map[string]map[string]interface{})
	accountMetadataDeletes := make(map[string][]string)

	// Accumulate idempotency entries for batch processing
	idempotencyEntries := make([]idempotencyEntry, 0)

	for _, log := range logs {
		// Validate log data
		if log.Data == nil {
			return fmt.Errorf("log data is nil for id %d", log.Id)
		}

		// Format date as RFC3339 string
		var dateStr string
		if log.Date != nil {
			dateStr = log.Date.AsTime().Format(stdtime.RFC3339)
		}

		// Accumulate idempotency entry if present
		if log.IdempotencyKey != "" && log.Id != 0 {
			idempotencyEntries = append(idempotencyEntries, idempotencyEntry{
				key:   log.IdempotencyKey,
				hash:  log.IdempotencyHash,
				logID: log.Id,
			})
		}

		// Accumulate balance differences and update accounts based on log type
		switch payload := log.Data.Payload.(type) {
		case *ledgerpb.LogPayload_CreatedTransaction:
			if payload.CreatedTransaction != nil && payload.CreatedTransaction.Transaction != nil {
				// Accumulate balance differences
				accumulateBalanceDiffs(balanceDiffs, payload.CreatedTransaction.Transaction.Postings)
				// Accumulate account and metadata updates for batch processing
				accumulateAccountsFromTransaction(
					accountMetadataBatch,
					payload.CreatedTransaction,
				)
			}
		case *ledgerpb.LogPayload_RevertedTransaction:
			if payload.RevertedTransaction != nil && payload.RevertedTransaction.RevertedTransaction != nil {
				// Reverse postings for balance update (subtract from destination, add to source)
				reversedPostings := make([]*ledgerpb.Posting, len(payload.RevertedTransaction.RevertedTransaction.Postings))
				for i, posting := range payload.RevertedTransaction.RevertedTransaction.Postings {
					if posting != nil {
						reversedPostings[i] = &ledgerpb.Posting{
							Source:      posting.Destination,
							Destination: posting.Source,
							Asset:       posting.Asset,
							Amount:      posting.Amount,
						}
					}
				}
				// Accumulate balance differences (reversed)
				accumulateBalanceDiffs(balanceDiffs, reversedPostings)
				// Accumulate account updates for batch processing
				accumulateAccountsFromRevertedTransaction(
					payload.RevertedTransaction,
					dateStr,
				)
			}
		case *ledgerpb.LogPayload_SavedMetadata:
			if payload.SavedMetadata != nil {
				// Accumulate metadata updates for batch processing
				accumulateMetadataFromSetMetadata(
					accountMetadataBatch,
					payload.SavedMetadata,
				)
			}
		case *ledgerpb.LogPayload_DeletedMetadata:
			if payload.DeletedMetadata != nil {
				// Accumulate metadata deletions for batch processing
				accumulateMetadataFromDeleteMetadata(
					accountMetadataDeletes,
					payload.DeletedMetadata,
				)
			}
		}
	}

	// Apply all accumulated balance differences in one batch
	if err := s.applyBalanceDiffs(ctx, tx, balanceDiffs); err != nil {
		return fmt.Errorf("applying balance differences: %w", err)
	}

	// Batch upsert all account metadata
	if len(accountMetadataBatch) > 0 {
		if err := s.batchUpsertAccountMetadata(ctx, tx, accountMetadataBatch); err != nil {
			return fmt.Errorf("batch upserting account metadata: %w", err)
		}
	}

	// Batch delete all account metadata keys
	if len(accountMetadataDeletes) > 0 {
		if err := s.batchDeleteAccountMetadataKeys(ctx, tx, accountMetadataDeletes); err != nil {
			return fmt.Errorf("batch deleting account metadata keys: %w", err)
		}
	}

	// Batch insert all idempotency entries
	if len(idempotencyEntries) > 0 {
		if err := s.batchInsertIdempotency(ctx, tx, idempotencyEntries); err != nil {
			return fmt.Errorf("batch inserting idempotency entries: %w", err)
		}
	}

	// Update last processed log ID (use the last log's ID)
	if len(logs) > 0 {
		lastLogID := logs[len(logs)-1].Id
		if err := s.updateLastProcessedLogID(ctx, tx, lastLogID); err != nil {
			return fmt.Errorf("updating last processed log ID: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Runtime data updated from logs")
	return nil
}

// applyBalanceDiffs applies accumulated balance differences to the database
func (s *SQLiteRuntimeStore) applyBalanceDiffs(ctx context.Context, tx *sql.Tx, balanceDiffs map[string]map[string]*big.Int) error {
	if len(balanceDiffs) == 0 {
		return nil
	}

	stmt := tx.StmtContext(ctx, s.stmtInsertBalance)
	defer func() {
		_ = stmt.Close()
	}()

	for account, assets := range balanceDiffs {
		for asset, balance := range assets {
			if _, err := stmt.ExecContext(ctx, account, asset, balance.String()); err != nil {
				return fmt.Errorf("updating balance for account %s asset %s: %w", account, asset, err)
			}
		}
	}

	return nil
}

// batchUpsertAccountMetadata inserts or updates account metadata for multiple accounts in a single batch operation
func (s *SQLiteRuntimeStore) batchUpsertAccountMetadata(ctx context.Context, tx *sql.Tx, accountMetadata map[string]map[string]interface{}) error {
	if len(accountMetadata) == 0 {
		return nil
	}

	stmt := tx.StmtContext(ctx, s.stmtUpsertAccountMetadata)
	defer func() {
		_ = stmt.Close()
	}()

	for accountAddr, metadataMap := range accountMetadata {
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

	return nil
}

// batchDeleteAccountMetadataKeys deletes multiple metadata keys for multiple accounts in a single batch operation
func (s *SQLiteRuntimeStore) batchDeleteAccountMetadataKeys(ctx context.Context, tx *sql.Tx, accountKeys map[string][]string) error {
	if len(accountKeys) == 0 {
		return nil
	}

	stmt := tx.StmtContext(ctx, s.stmtDeleteAccountMetadata)
	defer func() {
		_ = stmt.Close()
	}()

	for accountAddr, keys := range accountKeys {
		for _, key := range keys {
			if _, err := stmt.ExecContext(ctx, accountAddr, key); err != nil {
				return fmt.Errorf("deleting account metadata key: %w", err)
			}
		}
	}

	return nil
}

// batchInsertIdempotency inserts idempotency entries in a single batch operation
func (s *SQLiteRuntimeStore) batchInsertIdempotency(ctx context.Context, tx *sql.Tx, entries []idempotencyEntry) error {
	if len(entries) == 0 {
		return nil
	}

	stmt := tx.StmtContext(ctx, s.stmtInsertIdempotency)
	defer func() {
		_ = stmt.Close()
	}()

	for _, entry := range entries {
		if _, err := stmt.ExecContext(ctx, entry.key, entry.hash, entry.logID); err != nil {
			return fmt.Errorf("inserting idempotency entry for key %s: %w", entry.key, err)
		}
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
func (s *SQLiteRuntimeStore) GetLogForIdempotencyKey(ctx context.Context, idempotencyKey string) (string, uint64, error) {
	if idempotencyKey == "" {
		return "", 0, nil
	}

	var hash string
	var logID uint64

	err := s.stmtGetIdempotency.QueryRowContext(ctx, idempotencyKey).Scan(&hash, &logID)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", 0, nil
		}
		return "", 0, fmt.Errorf("querying idempotency entry: %w", err)
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

// updateLastProcessedLogID updates the last processed log ID in the infos table
func (s *SQLiteRuntimeStore) updateLastProcessedLogID(ctx context.Context, tx *sql.Tx, logID uint64) error {
	stmt := tx.StmtContext(ctx, s.stmtUpdateLastProcessedLogID)
	defer func() {
		_ = stmt.Close()
	}()

	valueStr := fmt.Sprintf("%d", logID)
	if _, err := stmt.ExecContext(ctx, valueStr); err != nil {
		return fmt.Errorf("updating last processed log ID: %w", err)
	}

	return nil
}
