package numscript

// DependencyResolutionError wraps a Numscript dependency-resolution failure with
// the provenance admission needs to classify it: whether resolution attempted a
// mutable balance/metadata lookup before failing. Unwrap exposes Cause so
// errors.Is / errors.As on the underlying error (the panic marker, a
// domain.Describable, a freezable kind, or a context error) stay transparent.
// See EN-1557: admission distinguishes deterministic no-read failures (terminal
// for inline/exact scripts) from state-dependent ones (forwardable under an
// idempotency key).
type DependencyResolutionError struct {
	Cause                error
	MutableReadAttempted bool
}

func (e *DependencyResolutionError) Error() string { return e.Cause.Error() }
func (e *DependencyResolutionError) Unwrap() error { return e.Cause }
