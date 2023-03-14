package errors

import (
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/lib/pq"
)

// postgresError is an helper to wrap postgres errors into storage errors
func PostgresError(err error) error {
	if err != nil {
		switch pge := err.(type) {
		case *pq.Error:
			switch pge.Code {
			case "23505":
				return storage.NewError(storage.ConstraintFailed, err)
			case "53300":
				return storage.NewError(storage.TooManyClient, err)
			}
		}
	}

	return err
}
