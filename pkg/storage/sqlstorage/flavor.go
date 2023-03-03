package sqlstorage

import (
	"errors"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgx/v5/pgconn"
)

type Flavor sqlbuilder.Flavor

var (
	PostgreSQL = Flavor(sqlbuilder.PostgreSQL)
)

func (f Flavor) String() string {
	switch f {
	case PostgreSQL:
		return "postgres"
	default:
		return "unknown"
	}
}

func postgresError(err error) error {
	var pgConnError *pgconn.PgError
	if errors.As(err, &pgConnError) {
		switch pgConnError.Code {
		case "23505":
			return storage.NewError(storage.ConstraintFailed, err)
		case "53300":
			return storage.NewError(storage.TooManyClient, err)
		}
	}
	return err
}
