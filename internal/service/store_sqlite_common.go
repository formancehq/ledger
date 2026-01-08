package service

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/XSAM/otelsql"
	_ "github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/otel/metric"
	_ "modernc.org/sqlite"
)

type SQLDB struct {
	*sql.DB
	reg metric.Registration
}

func (db *SQLDB) Close() error {
	if err := db.reg.Unregister(); err != nil {
		return err
	}
	return db.DB.Close()
}

// SQLiteMetrics contains SQLite database metrics
type SQLiteMetrics struct {
	PageCount     int64 `json:"pageCount"`
	PageSize      int64 `json:"pageSize"`
	DatabaseSize  int64 `json:"databaseSize"`
	FreePages     int64 `json:"freePages"`
	SchemaVersion int64 `json:"schemaVersion"`
}

// getSQLiteMetrics retrieves SQLite database metrics using PRAGMA statements
func getSQLiteMetrics(db *SQLDB) *SQLiteMetrics {
	ctx := context.Background()
	metrics := &SQLiteMetrics{}

	// Get page count
	var pageCount int64
	err := db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pageCount)
	if err == nil {
		metrics.PageCount = pageCount
	}

	// Get page size
	var pageSize int64
	err = db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize)
	if err == nil {
		metrics.PageSize = pageSize
		metrics.DatabaseSize = pageCount * pageSize
	}

	// Get free pages
	var freePages int64
	err = db.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&freePages)
	if err == nil {
		metrics.FreePages = freePages
	}

	// Get schema version
	var schemaVersion int64
	err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schemaVersion)
	if err == nil {
		metrics.SchemaVersion = schemaVersion
	}

	return metrics
}

// openSQLiteModernDB opens a SQLite database using the modernc.org/sqlite driver
func openSQLiteModernDB(dsn string, options ...otelsql.Option) (*SQLDB, error) {
	db, err := otelsql.Open("sqlite",
		dsn+"?cache=shared&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=temp_store(MEMORY)&_pragma=cache_size(-32768)",
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite modern database: %w", err)
	}
	return configureSQLiteDB(db, options...)
}

// openSQLiteMattnDB opens a SQLite database using the github.com/mattn/go-sqlite3 driver
func openSQLiteMattnDB(dsn string, options ...otelsql.Option) (*SQLDB, error) {
	db, err := otelsql.Open("sqlite3", dsn+
		"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=-32768&_temp_store=MEMORY&_busy_timeout=5000&_txlock=immediate",
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database: %w", err)
	}
	return configureSQLiteDB(db, options...)
}

// configureSQLiteDB configures common SQLite database connection settings
func configureSQLiteDB(db *sql.DB, options ...otelsql.Option) (*SQLDB, error) {
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	reg, err := otelsql.RegisterDBStatsMetrics(db, options...)
	if err != nil {
		panic(err)
	}

	return &SQLDB{db, reg}, nil
}
