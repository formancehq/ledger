package errorsutil

// Error is a wrapper that allows to wrap an error with another error.
// Let's take an example: imagine that you have multiple errors in a storage
// package, and you want to wrap them with a storage error. You will be able to
// do it with this wrapper and not loose the original error.
type Error struct {
	wrappingError error
	originalErr   error
}

// Error returns the original error.
func (e *Error) Error() string {
	return e.originalErr.Error()
}

// Implements the Causer interface from the github.com/pkg/errors package.
func (e *Error) Cause() error {
	return e.originalErr
}

// Unwrap returns the original error in order to be able to use the errors.Is
// function easily.
func (e *Error) Unwrap() error {
	return e.originalErr
}

// Is implements the Is interface of errors
func (e *Error) Is(err error) bool {
	return e.wrappingError == err
}

func NewError(wrappingError, originalErr error) *Error {
	return &Error{
		wrappingError: wrappingError,
		originalErr:   originalErr,
	}
}
