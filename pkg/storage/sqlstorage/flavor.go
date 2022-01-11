package sqlstorage

import (
	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgconn"
	"github.com/mattn/go-sqlite3"
	"github.com/numary/ledger/pkg/storage"
)

type Flavor = sqlbuilder.Flavor

var (
	SQLite     = sqlbuilder.SQLite
	PostgreSQL = sqlbuilder.PostgreSQL
)

func errorFromFlavor(f Flavor, err error) error {
	if err == nil {
		return nil
	}
	switch f {
	case SQLite:
		eerr, ok := err.(sqlite3.Error)
		if !ok {
			return err
		}
		if eerr.Code == sqlite3.ErrConstraint {
			return storage.NewError(storage.ConstraintFailed, err)
		}
	case PostgreSQL:
		eerr, ok := err.(*pgconn.PgError)
		if !ok {
			return err
		}
		switch eerr.Code {
		case "23505":
			return storage.NewError(storage.ConstraintFailed, err)
		}
	}
	return err
}
