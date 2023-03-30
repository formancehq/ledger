package storage

import (
	"github.com/pkg/errors"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrConstraintFailed    = errors.New("23505: constraint failed")
	ErrTooManyClients      = errors.New("53300: too many clients")
	ErrJson                = errors.New("json marshal/unmarshal error")
	ErrParsingBalance      = errors.New("parsing balance error")
	ErrStoreNotInitialized = errors.New("store not initialized")
)

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsContraintFailed(err error) bool {
	return errors.Is(err, ErrConstraintFailed)
}

func IsErrTooManyClients(err error) bool {
	return errors.Is(err, ErrTooManyClients)
}

func IsErrJson(err error) bool {
	return errors.Is(err, ErrJson)
}

func IsErrParsingBalance(err error) bool {
	return errors.Is(err, ErrParsingBalance)
}

func IsStoreNotInitialized(err error) bool {
	return errors.Is(err, ErrStoreNotInitialized)
}
