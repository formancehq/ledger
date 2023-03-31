package storage

import (
	"github.com/pkg/errors"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrConstraintFailed    = errors.New("23505: constraint failed")
	ErrTooManyClients      = errors.New("53300: too many clients")
	ErrStoreNotInitialized = errors.New("store not initialized")

	ErrStorage = errors.New("storage error")
)

func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsContraintFailed(err error) bool {
	return errors.Is(err, ErrConstraintFailed)
}

func IsErrTooManyClients(err error) bool {
	return errors.Is(err, ErrTooManyClients)
}

func IsStoreNotInitialized(err error) bool {
	return errors.Is(err, ErrStoreNotInitialized)
}

func IsStorageError(err error) bool {
	return errors.Is(err, ErrStorage)
}
