package sqlutils

import (
	"database/sql"

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
				return newErrConstraintsFailed(err)
			case "53300":
				return newErrTooManyClient(err)
			}
		}

		return err
	}

	return nil
}

var (
	ErrNotFound           = errors.New("not found")
	ErrStoreAlreadyExists = errors.New("store already exists")
	ErrStoreNotFound      = errors.New("store not found")
)

func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

type errConstraintsFailed struct {
	err error
}

func (e errConstraintsFailed) Error() string {
	return e.err.Error()
}

func newErrConstraintsFailed(err error) *errConstraintsFailed {
	return &errConstraintsFailed{
		err: err,
	}
}

type errTooManyClient struct {
	err error
}

func (e errTooManyClient) Error() string {
	return e.err.Error()
}

func newErrTooManyClient(err error) *errTooManyClient {
	return &errTooManyClient{
		err: err,
	}
}
