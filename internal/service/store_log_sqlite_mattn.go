package service

import (
	"github.com/XSAM/otelsql"
	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/attribute"

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
func NewSQLiteMattnLogStore(dsn string, logger logging.Logger) (*SQLiteLogStore, error) {
	db, err := openSQLiteMattnDB(dsn, otelsql.WithAttributes(
		attribute.String("store_type", "log-store"),
	))
	if err != nil {
		return nil, err
	}

	store, err := NewSQLiteLogStore(db, logger)
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}

	return store, nil
}
