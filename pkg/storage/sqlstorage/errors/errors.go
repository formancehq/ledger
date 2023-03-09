package errors

import (
	"errors"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/jackc/pgx/v5/pgconn"
)

// postgresError is an helper to wrap postgres errors into storage errors
func PostgresError(err error) error {
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
