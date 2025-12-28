package service

import (
	"github.com/formancehq/go-libs/v3/logging"

	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// ============================================================================
// Types and Structures
// ============================================================================

// SQLiteMattnConfig represents the configuration for a SQLite Mattn bucket driver
type SQLiteMattnConfig struct {
	DSN string `json:"dsn"` // Data Source Name (connection string)
}

// NewSQLiteMattnLogStore creates a new SQLite log store using github.com/mattn/go-sqlite3
func NewSQLiteMattnLogStore(ctx context.Context, dsn string, logger logging.Logger) (*SQLiteLogStore, error) {
	// Open SQLite database using sqlite3 driver
	db, err := sql.Open("sqlite3", dsn+
		"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=-32768&_temp_store=MEMORY&_busy_timeout=5000&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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
