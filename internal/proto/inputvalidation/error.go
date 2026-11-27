// Package inputvalidation contains errors returned by generated input validators.
package inputvalidation

import "errors"

// FieldError identifies a direct input field that violates a declared rule.
type FieldError struct {
	Path string
	Rule string
}

func (e *FieldError) Error() string {
	switch e.Rule {
	case "max_bytes":
		return e.Path + " exceeds its maximum byte length"
	case "exact_bytes":
		return e.Path + " has an invalid byte length"
	default:
		return e.Path + " is required"
	}
}

// MustMatch rejects a stale caller mapping when a generated validator gains
// another field or rule. Call it before translating an input error to a
// field-specific public error.
func MustMatch(err error, path, rule string) {
	if err == nil {
		panic("unexpected nil generated input validation error")
	}
	var fieldErr *FieldError
	if !errors.As(err, &fieldErr) {
		panic("unexpected generated input validation error type")
	}
	if fieldErr.Path != path || fieldErr.Rule != rule {
		panic("unexpected generated input validation field or rule")
	}
}
