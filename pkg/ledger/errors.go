package ledger

import (
	"github.com/pkg/errors"
)

var (
	ErrValidation = errors.New("validation error")
)

func IsValidationError(err error) bool {
	return errors.Is(err, ErrValidation)
}
