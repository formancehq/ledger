package errors

import (
	"database/sql"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// postgresError is an helper to wrap postgres errors into storage errors
func PostgresError(err error) error {
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrNotFound
		}

		switch pge := err.(type) {
		case *pq.Error:
			switch pge.Code {
			case "23505":
				return storage.NewError(storage.ErrConstraintFailed, err)
			case "53300":
				return storage.NewError(storage.ErrTooManyClient, err)
			}
		}
	}

	return err
}
