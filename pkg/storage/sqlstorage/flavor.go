package sqlstorage

import (
	"errors"

	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgconn"
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

		handleError := func(err error) error {
			switch eerr := err.(type) {
			case *pgconn.PgError:
				switch eerr.Code {
				case "23505":
					return storage.NewError(storage.ConstraintFailed, err)
				case "53300":
					return storage.NewError(storage.TooManyClient, err)
				}
			}
			return err
		}

		unwrappedError := errors.Unwrap(err)
		if unwrappedError != nil {
			return handleError(unwrappedError)
		} else {
			return handleError(err)
		}
	}
}
