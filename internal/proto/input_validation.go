package proto

// ValidationError is the classified result of a generated input validator.
type ValidationError interface {
	error
	Kind() ErrorKind
	Reason() string
}

// FieldError describes the protobuf field rejected at admission.
type FieldError struct {
	Message       string
	Path          string
	Rule          string
	PublicMessage string
	Code          string
}

func (e *FieldError) Error() string { return e.PublicMessage }
func (*FieldError) Kind() ErrorKind { return KindValidation }
func (*FieldError) Reason() string  { return "VALIDATION" }
func (e *FieldError) PublicDetails() (string, map[string]string, bool) {
	return e.PublicMessage, nil, true
}
func (e *FieldError) ValidationCode() string { return e.Code }
func (e *FieldError) Is(target error) bool {
	other, ok := target.(interface{ ValidationCode() string })

	return ok && e.Code != "" && e.Code == other.ValidationCode()
}
