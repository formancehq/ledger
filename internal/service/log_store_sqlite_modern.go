package service

import (
	"github.com/formancehq/go-libs/v3/logging"

	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// ============================================================================
// Types and Structures
// ============================================================================

// SQLiteModernConfig represents the configuration for a SQLite Modern bucket driver
type SQLiteModernConfig struct {
	DSN string `json:"dsn"` // Data Source Name (connection string)
}

// NewSQLiteModernLogStore creates a new SQLite Modern log store
func NewSQLiteModernLogStore(ctx context.Context, dsn string, logger logging.Logger) (*SQLiteLogStore, error) {
	// Open SQLite database
	db, err := sql.Open("sqlite", dsn+
		"?cache=shared&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=temp_store(MEMORY)&_pragma=cache_size(-32768)")
	if err != nil {
		return nil, fmt.Errorf("opening sqlite modern database: %w", err)
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
