package query

import "github.com/pkg/errors"

var (
	ErrStorage = errors.New("storage error")
)

func IsStorageError(err error) bool {
	return errors.Is(err, ErrStorage)
}
