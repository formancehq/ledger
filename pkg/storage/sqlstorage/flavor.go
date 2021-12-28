package sqlstorage

import (
	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgconn"
	"github.com/mattn/go-sqlite3"
	"github.com/numary/ledger/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type Flavor sqlbuilder.Flavor

var (
	SQLite     = Flavor(sqlbuilder.SQLite)
	PostgreSQL = Flavor(sqlbuilder.PostgreSQL)
)

func (f Flavor) String() string {
	switch f {
	case SQLite:
		return "sqlite"
	case PostgreSQL:
		return "postgres"
	default:
		return "unknown"
	}
}

func (f Flavor) AttributeKeyValue() attribute.KeyValue {
	switch f {
	case SQLite:
		return semconv.DBSystemSqlite
	case PostgreSQL:
		return semconv.DBSystemPostgreSQL
	default:
		return attribute.KeyValue{}
	}
}

func FlavorFromString(v string) Flavor {
	switch v {
	case "sqlite":
		return SQLite
	case "postgres":
		return PostgreSQL
	default:
		return 0
	}
}
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
