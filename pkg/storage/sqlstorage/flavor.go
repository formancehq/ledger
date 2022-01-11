package sqlstorage

import (
	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgconn"
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

var errorHandlers = map[Flavor]func(error) error{}

func errorFromFlavor(f Flavor, err error) error {
	if err == nil {
		return nil
	}
	h, ok := errorHandlers[f]
	if !ok {
		return err
	}
	return h(err)
}

func init() {
	errorHandlers[PostgreSQL] = func(err error) error {
		eerr, ok := err.(*pgconn.PgError)
		if !ok {
			return err
		}
		switch eerr.Code {
		case "23505":
			return storage.NewError(storage.ConstraintFailed, err)
		}
		return err
	}
}
