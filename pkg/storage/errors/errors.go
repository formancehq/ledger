package errors

import (
	"database/sql"

	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// postgresError is an helper to wrap postgres errors into storage errors
func PostgresError(err error) error {
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}

		switch pge := err.(type) {
		case *pq.Error:
			switch pge.Code {
			case "23505":
				return errorsutil.NewError(ErrStorage,
					errorsutil.NewError(ErrConstraintFailed, err))
			case "53300":
				return errorsutil.NewError(ErrStorage,
					errorsutil.NewError(ErrTooManyClients, err))
			}
		}

		return errorsutil.NewError(ErrStorage, err)
	}

	return nil
}

func StorageError(err error) error {
	if err == nil {
		return nil
	}

	return errorsutil.NewError(ErrStorage, err)
}

var ErrNotFound = errors.New("not found")

var (
	ErrConstraintFailed    = errors.New("23505: constraint failed")
	ErrTooManyClients      = errors.New("53300: too many clients")
	ErrStoreNotInitialized = errors.New("store not initialized")
	ErrStoreAlreadyExists  = errors.New("store already exists")
	ErrStoreNotFound       = errors.New("store not found")

	ErrStorage = errors.New("storage error")
)

func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsStorageError(err error) bool {
	return errors.Is(err, ErrStorage)
}
