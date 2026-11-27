// Package inputvalidation contains errors returned by generated input validators.
package inputvalidation

// FieldError identifies a direct input field that violates a declared rule.
type FieldError struct {
	Message       string
	Path          string
	Rule          string
	PublicMessage string
}

func (e *FieldError) Error() string {
	if e.PublicMessage != "" {
		return e.PublicMessage
	}
	switch e.Rule {
	case "max_bytes":
		return e.Path + " exceeds its maximum byte length"
	case "exact_bytes":
		return e.Path + " has an invalid byte length"
	default:
		return e.Path + " is required"
	}
}
