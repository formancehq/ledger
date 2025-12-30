package service

import (
	"github.com/XSAM/otelsql"
	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/attribute"

	"context"
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
	db, err := openSQLiteModernDB(dsn, otelsql.WithAttributes(
		attribute.String("store.type", "log-store"),
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
