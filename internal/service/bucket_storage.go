package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

// BalancesStore handles persistent storage of balances
type BalancesStore interface {
	// SaveBalances saves balances for a ledger
	SaveBalances(ctx context.Context, ledgerName string, balances ledger.Balances) error
	// GetBalances retrieves balances for a ledger
	GetBalances(ctx context.Context, ledgerName string) (ledger.Balances, error)
}

// AccountMetadataStore handles persistent storage of account metadata
type AccountMetadataStore interface {
	// SaveAccountMetadata saves account metadata for a ledger
	SaveAccountMetadata(ctx context.Context, ledgerName string, accountMetadata map[string]map[string]string) error
	// GetAccountMetadata retrieves account metadata for a ledger
	GetAccountMetadata(ctx context.Context, ledgerName string) (map[string]map[string]string, error)
}

// BucketStorage is an interface for bucket storage backends
type BucketStorage interface {
	BalancesStore
	AccountMetadataStore
	// Close closes the storage and releases any resources
	Close() error
}

// ValidateBucketConfig validates the configuration for a bucket driver
func ValidateBucketConfig(driver string, config map[string]interface{}) error {
	switch driver {
	case "sqlite":
		dsn, ok := config["dsn"].(string)
		if !ok || dsn == "" {
			return fmt.Errorf("sqlite driver requires 'dsn' configuration (connection address)")
		}
		return nil
	case "file":
		path, ok := config["path"].(string)
		if !ok || path == "" {
			return fmt.Errorf("file driver requires 'path' configuration (directory path)")
		}
		return nil
	default:
		return fmt.Errorf("unsupported driver: %s (supported drivers: sqlite, file)", driver)
	}
}

// NewBucketStorage creates a new bucket storage based on the driver and configuration
func NewBucketStorage(ctx context.Context, driver string, config map[string]interface{}, logger *zap.Logger) (BucketStorage, error) {
	// Validate configuration
	if err := ValidateBucketConfig(driver, config); err != nil {
		return nil, err
	}

	switch driver {
	case "sqlite":
		dsn := config["dsn"].(string)
		return NewSQLiteBucketStorage(ctx, dsn, logger)
	case "file":
		path := config["path"].(string)
		return NewFileBucketStorage(path, logger)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
}

// SQLiteBucketStorage stores bucket data in SQLite
type SQLiteBucketStorage struct {
	dsn    string
	db     *sql.DB
	logger *zap.Logger
}

// NewSQLiteBucketStorage creates a new SQLite bucket storage
func NewSQLiteBucketStorage(ctx context.Context, dsn string, logger *zap.Logger) (*SQLiteBucketStorage, error) {
	// Ensure the directory exists if dsn is a file path
	if dsn != ":memory:" {
		dir := filepath.Dir(dsn)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("creating directory for SQLite database: %w", err)
		}
	}

	// Open SQLite database
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database: %w", err)
	}

	storage := &SQLiteBucketStorage{
		dsn:    dsn,
		db:     db,
		logger: logger.With(zap.String("driver", "sqlite"), zap.String("dsn", dsn)),
	}

	// Create tables if they don't exist
	if err := storage.createTables(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating tables: %w", err)
	}

	storage.logger.Info("SQLite bucket storage initialized")
	return storage, nil
}

// createTables creates the necessary tables for balances and account metadata
func (s *SQLiteBucketStorage) createTables(ctx context.Context) error {
	// Create balances table: ledger -> account -> asset -> balance
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS balances (
			ledger TEXT NOT NULL,
			account TEXT NOT NULL,
			asset TEXT NOT NULL,
			balance TEXT NOT NULL,
			PRIMARY KEY (ledger, account, asset)
		);
		
		CREATE INDEX IF NOT EXISTS idx_balances_ledger ON balances(ledger);
	`)

	// Create account_metadata table: ledger -> account -> key -> value
	_, err2 := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS account_metadata (
			ledger TEXT NOT NULL,
			account TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (ledger, account, key)
		);
		
		CREATE INDEX IF NOT EXISTS idx_account_metadata_ledger ON account_metadata(ledger);
	`)

	if err != nil {
		return fmt.Errorf("creating balances table: %w", err)
	}
	if err2 != nil {
		return fmt.Errorf("creating account_metadata table: %w", err2)
	}

	return nil
}

// SaveBalances saves balances for a ledger
func (s *SQLiteBucketStorage) SaveBalances(ctx context.Context, ledgerName string, balances ledger.Balances) error {
	// Use a transaction for batch insert
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete existing balances for this ledger
	_, err = tx.ExecContext(ctx, `DELETE FROM balances WHERE ledger = ?`, ledgerName)
	if err != nil {
		return fmt.Errorf("deleting existing balances: %w", err)
	}

	// Insert new balances
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO balances (ledger, account, asset, balance) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %w", err)
	}
	defer stmt.Close()

	for account, assets := range balances {
		for asset, balance := range assets {
			if _, err := stmt.ExecContext(ctx, ledgerName, account, asset, balance.String()); err != nil {
				return fmt.Errorf("inserting balance: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// GetBalances retrieves balances for a ledger
func (s *SQLiteBucketStorage) GetBalances(ctx context.Context, ledgerName string) (ledger.Balances, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT account, asset, balance FROM balances WHERE ledger = ?`, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("querying balances: %w", err)
	}
	defer rows.Close()

	balances := make(ledger.Balances)
	for rows.Next() {
		var account, asset, balanceStr string
		if err := rows.Scan(&account, &asset, &balanceStr); err != nil {
			return nil, fmt.Errorf("scanning balance: %w", err)
		}

		balance, ok := new(big.Int).SetString(balanceStr, 10)
		if !ok {
			return nil, fmt.Errorf("invalid balance format: %s", balanceStr)
		}

		if balances[account] == nil {
			balances[account] = make(map[string]*big.Int)
		}
		balances[account][asset] = balance
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating balances: %w", err)
	}

	return balances, nil
}

// SaveAccountMetadata saves account metadata for a ledger
func (s *SQLiteBucketStorage) SaveAccountMetadata(ctx context.Context, ledgerName string, accountMetadata map[string]map[string]string) error {
	// Use a transaction for batch insert
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete existing account metadata for this ledger
	_, err = tx.ExecContext(ctx, `DELETE FROM account_metadata WHERE ledger = ?`, ledgerName)
	if err != nil {
		return fmt.Errorf("deleting existing account metadata: %w", err)
	}

	// Insert new account metadata
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO account_metadata (ledger, account, key, value) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %w", err)
	}
	defer stmt.Close()

	for account, metadata := range accountMetadata {
		for key, value := range metadata {
			if _, err := stmt.ExecContext(ctx, ledgerName, account, key, value); err != nil {
				return fmt.Errorf("inserting account metadata: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// GetAccountMetadata retrieves account metadata for a ledger
func (s *SQLiteBucketStorage) GetAccountMetadata(ctx context.Context, ledgerName string) (map[string]map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT account, key, value FROM account_metadata WHERE ledger = ?`, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("querying account metadata: %w", err)
	}
	defer rows.Close()

	accountMetadata := make(map[string]map[string]string)
	for rows.Next() {
		var account, key, value string
		if err := rows.Scan(&account, &key, &value); err != nil {
			return nil, fmt.Errorf("scanning account metadata: %w", err)
		}

		if accountMetadata[account] == nil {
			accountMetadata[account] = make(map[string]string)
		}
		accountMetadata[account][key] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating account metadata: %w", err)
	}

	return accountMetadata, nil
}

// Close closes the SQLite bucket storage
func (s *SQLiteBucketStorage) Close() error {
	s.logger.Debug("Closing SQLite bucket storage")
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// FileBucketStorage stores bucket data in a file directory
type FileBucketStorage struct {
	path   string
	logger *zap.Logger
}

// NewFileBucketStorage creates a new file bucket storage
func NewFileBucketStorage(path string, logger *zap.Logger) (*FileBucketStorage, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("creating directory for file bucket storage: %w", err)
	}

	storage := &FileBucketStorage{
		path:   path,
		logger: logger.With(zap.String("driver", "file"), zap.String("path", path)),
	}

	storage.logger.Info("File bucket storage initialized")
	return storage, nil
}

// getBalancesPath returns the path to the balances file for a ledger
func (f *FileBucketStorage) getBalancesPath(ledgerName string) string {
	return filepath.Join(f.path, fmt.Sprintf("%s-balances.json", ledgerName))
}

// getAccountMetadataPath returns the path to the account metadata file for a ledger
func (f *FileBucketStorage) getAccountMetadataPath(ledgerName string) string {
	return filepath.Join(f.path, fmt.Sprintf("%s-account-metadata.json", ledgerName))
}

// SaveBalances saves balances for a ledger
func (f *FileBucketStorage) SaveBalances(ctx context.Context, ledgerName string, balances ledger.Balances) error {
	// Convert balances to JSON-serializable format
	balancesJSON := make(map[string]map[string]string)
	for account, assets := range balances {
		balancesJSON[account] = make(map[string]string)
		for asset, balance := range assets {
			balancesJSON[account][asset] = balance.String()
		}
	}

	data, err := json.Marshal(balancesJSON)
	if err != nil {
		return fmt.Errorf("marshaling balances: %w", err)
	}

	filePath := f.getBalancesPath(ledgerName)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("writing balances file: %w", err)
	}

	return nil
}

// GetBalances retrieves balances for a ledger
func (f *FileBucketStorage) GetBalances(ctx context.Context, ledgerName string) (ledger.Balances, error) {
	filePath := f.getBalancesPath(ledgerName)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(ledger.Balances), nil
		}
		return nil, fmt.Errorf("reading balances file: %w", err)
	}

	var balancesJSON map[string]map[string]string
	if err := json.Unmarshal(data, &balancesJSON); err != nil {
		return nil, fmt.Errorf("unmarshaling balances: %w", err)
	}

	balances := make(ledger.Balances)
	for account, assets := range balancesJSON {
		balances[account] = make(map[string]*big.Int)
		for asset, balanceStr := range assets {
			balance, ok := new(big.Int).SetString(balanceStr, 10)
			if !ok {
				return nil, fmt.Errorf("invalid balance format: %s", balanceStr)
			}
			balances[account][asset] = balance
		}
	}

	return balances, nil
}

// SaveAccountMetadata saves account metadata for a ledger
func (f *FileBucketStorage) SaveAccountMetadata(ctx context.Context, ledgerName string, accountMetadata map[string]map[string]string) error {
	data, err := json.Marshal(accountMetadata)
	if err != nil {
		return fmt.Errorf("marshaling account metadata: %w", err)
	}

	filePath := f.getAccountMetadataPath(ledgerName)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("writing account metadata file: %w", err)
	}

	return nil
}

// GetAccountMetadata retrieves account metadata for a ledger
func (f *FileBucketStorage) GetAccountMetadata(ctx context.Context, ledgerName string) (map[string]map[string]string, error) {
	filePath := f.getAccountMetadataPath(ledgerName)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]map[string]string), nil
		}
		return nil, fmt.Errorf("reading account metadata file: %w", err)
	}

	var accountMetadata map[string]map[string]string
	if err := json.Unmarshal(data, &accountMetadata); err != nil {
		return nil, fmt.Errorf("unmarshaling account metadata: %w", err)
	}

	return accountMetadata, nil
}

// Close closes the file bucket storage
func (f *FileBucketStorage) Close() error {
	f.logger.Debug("Closing file bucket storage")
	return nil
}

// GetPath returns the storage path (for file storage)
func (f *FileBucketStorage) GetPath() string {
	return f.path
}
