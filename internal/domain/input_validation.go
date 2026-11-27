package domain

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/inputvalidation"
)

// InputValidation presents a generated structural error as a validation error
// without an application-owned field lookup table.
func InputValidation(err error) Describable {
	var field *inputvalidation.FieldError
	if !errors.As(err, &field) || field.PublicMessage == "" {
		panic("unexpected generated input validation error")
	}

	return inputValidationError{field: field}
}

type inputValidationError struct {
	field *inputvalidation.FieldError
}

func (e inputValidationError) Error() string { return e.field.Error() }
func (e inputValidationError) Unwrap() error { return e.field }
func (inputValidationError) Kind() ErrorKind { return KindValidation }
func (inputValidationError) Reason() string  { return ErrReasonValidation }
func (e inputValidationError) PublicDetails() (string, map[string]string, bool) {
	return e.field.PublicMessage, nil, true
}

// Existing validation sentinels remain matchable by their stable public
// presentation while generated errors replace field-specific lookup tables.
func (e inputValidationError) Is(target error) bool {
	d, ok := target.(Describable)

	return ok && d.Kind() == KindValidation && d.Reason() == ErrReasonValidation && d.Error() == e.field.PublicMessage
}
