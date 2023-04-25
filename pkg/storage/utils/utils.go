package utils

import (
	"database/sql"
	"io"
	"os"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bundebug"
)

type ConnectionOptions struct {
	DatabaseSourceName string
	Debug              bool
	Writer             io.Writer
	MaxIdleConns       int
	MaxOpenConns       int
	ConnMaxIdleTime    time.Duration
}

func OpenSQLDB(options ConnectionOptions) (*bun.DB, error) {
	sqldb, err := sql.Open("postgres", options.DatabaseSourceName)
	if err != nil {
		return nil, err
	}
	if options.MaxIdleConns != 0 {
		sqldb.SetMaxIdleConns(options.MaxIdleConns)
	}
	if options.ConnMaxIdleTime != 0 {
		sqldb.SetConnMaxIdleTime(options.ConnMaxIdleTime)
	}
	if options.MaxOpenConns != 0 {
		sqldb.SetMaxOpenConns(options.MaxOpenConns)
	}

	db := bun.NewDB(sqldb, pgdialect.New())
	if options.Debug {
		writer := options.Writer
		if writer == nil {
			writer = os.Stdout
		}
		db.AddQueryHook(bundebug.NewQueryHook(
			bundebug.WithVerbose(true),
			bundebug.WithWriter(writer),
		))
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
