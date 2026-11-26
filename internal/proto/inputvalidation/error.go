// Package inputvalidation contains errors returned by generated input validators.
package inputvalidation

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
