package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/XSAM/otelsql"
	_ "github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/otel/metric"
	_ "modernc.org/sqlite"
)

type DB struct {
	*sql.DB
	reg metric.Registration
}

func (db *DB) Close() error {
	if err := db.reg.Unregister(); err != nil {
		return err
	}
	return db.DB.Close()
}

// OpenModernDB opens a SQLite database using the modernc.org/sqlite driver
func OpenModernDB(dsn string, options ...otelsql.Option) (*DB, error) {
	db, err := otelsql.Open("sqlite",
		dsn+"?cache=shared&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=temp_store(MEMORY)&_pragma=cache_size(-32768)",
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite modern database: %w", err)
	}
	return configureDB(db, options...)
}

// OpenMattnDB opens a SQLite database using the github.com/mattn/go-sqlite3 driver
func OpenMattnDB(dsn string, options ...otelsql.Option) (*DB, error) {
	db, err := otelsql.Open("sqlite3", dsn+
		"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=-32768&_temp_store=MEMORY&_busy_timeout=5000&_txlock=immediate",
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database: %w", err)
	}
	return configureDB(db, options...)
}

// configureDB configures common SQLite database connection settings
func configureDB(db *sql.DB, options ...otelsql.Option) (*DB, error) {
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	reg, err := otelsql.RegisterDBStatsMetrics(db, options...)
	if err != nil {
		panic(err)
	}

	return &DB{db, reg}, nil
}
