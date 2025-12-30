package service

import (
	"github.com/formancehq/go-libs/v3/logging"

	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// NewSQLiteMattnRuntimeStore creates a new SQLite Runtime store using github.com/mattn/go-sqlite3
func NewSQLiteMattnRuntimeStore(ctx context.Context, dsn string, logger logging.Logger) (*SQLiteRuntimeStore, error) {
	db, err := openSQLiteMattnDB(dsn)
	if err != nil {
		return nil, err
	}

	store, err := NewSQLiteRuntimeStore(db, logger)
	if err != nil {
		return nil, fmt.Errorf("creating runtime store: %w", err)
	}

	return store, nil
}

