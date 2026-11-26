// Package inputvalidation contains errors returned by generated input validators.
package inputvalidation

// FieldError identifies a missing required input field.
type FieldError struct {
	Path string
}

func (e *FieldError) Error() string { return e.Path + " is required" }
