// Package inputvalidation contains errors returned by generated input validators.
package inputvalidation

import "errors"

// FieldError identifies a direct input field that violates a declared rule.
type FieldError struct {
	Message string
	Path    string
	Rule    string
}

// Key identifies one protobuf input rule independently of its caller.
type Key struct {
	Message string
	Path    string
	Rule    string
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

// Resolve translates a generated error through one boundary's public-error
// table. An unmapped rule is an implementation failure, never an old field's
// public error.
func Resolve[T any](err error, mappings map[Key]T) T {
	if err == nil {
		panic("unexpected nil generated input validation error")
	}
	var fieldErr *FieldError
	if !errors.As(err, &fieldErr) {
		panic("unexpected generated input validation error type")
	}
	value, ok := mappings[Key{Message: fieldErr.Message, Path: fieldErr.Path, Rule: fieldErr.Rule}]
	if !ok {
		panic("unmapped generated input validation rule")
	}
	return value
}
