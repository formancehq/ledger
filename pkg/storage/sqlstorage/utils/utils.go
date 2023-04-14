package utils

import (
	"database/sql"
	"io"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bundebug"
)

func OpenSQLDB(dataSourceName string, debug bool, w io.Writer) (*bun.DB, error) {
	sqldb, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}

	db := bun.NewDB(sqldb, pgdialect.New())
	if debug {
		db.AddQueryHook(bundebug.NewQueryHook(
			bundebug.WithVerbose(true),
			bundebug.WithWriter(w),
		))
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
