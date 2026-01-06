package service

import (
	"github.com/XSAM/otelsql"
	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/attribute"

	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// NewSQLiteMattnRuntimeStore creates a new SQLite Runtime store using github.com/mattn/go-sqlite3
func NewSQLiteMattnRuntimeStore(ctx context.Context, dsn string, logger logging.Logger) (*SQLiteRuntimeStore, error) {
	db, err := openSQLiteMattnDB(dsn, otelsql.WithAttributes(
		attribute.String("store.type", "runtime"),
	))
	if err != nil {
		return nil, err
	}

	store, err := NewSQLiteRuntimeStore(db, logger)
	if err != nil {
		return nil, fmt.Errorf("creating runtime store: %w", err)
	}

	return store, nil
}

