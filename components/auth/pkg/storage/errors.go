package storage

import (
	"errors"
)

var (
	ErrNotFound = errors.New("not found")
)

func IgnoreNotFoundError(err error) error {
	if err == nil || err == ErrNotFound {
		return nil
	}
	return err
}
