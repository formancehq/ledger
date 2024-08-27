package sqlstorage

import (
	"errors"

	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/numary/ledger/pkg/storage"
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
		var pgConnError *pgconn.PgError
		if errors.As(err, &pgConnError) {
			switch pgConnError.Code {
			case "23505":
				if pgConnError.ConstraintName == "transactions_id_key" {
					return storage.NewError(storage.ConstraintTXID, err)
				}
				return storage.NewError(storage.ConstraintFailed, err)
			case "53300":
				return storage.NewError(storage.TooManyClient, err)
			}
		}
		return err
	}
}
