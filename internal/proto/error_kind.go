package proto

import "fmt"

type ErrorKind int

const (
	// KindValidation: the caller sent a syntactically/structurally invalid
	// request. gRPC: InvalidArgument. HTTP: 400.
	KindValidation ErrorKind = iota + 1

	// KindNotFound: the named resource does not exist. gRPC: NotFound.
	// HTTP: 404.
	KindNotFound

	// KindAlreadyExists: the resource exists and the caller asked to create
	// it. gRPC: AlreadyExists. HTTP: 409.
	KindAlreadyExists

	// KindConflict: the request collides with current state in a way that
	// could be reconciled by inspecting/modifying that state (e.g. already
	// reverted, ledger deleted, mirror-mode write attempt). gRPC:
	// FailedPrecondition. HTTP: 409.
	KindConflict

	// KindPrecondition: a precondition the caller can in principle satisfy
	// is not met (e.g. insufficient funds, index missing, transaction not
	// closing). gRPC: FailedPrecondition. HTTP: 400.
	KindPrecondition

	// KindUnavailable: the server cannot serve the request right now but
	// the caller should retry (e.g. maintenance mode, stale proposal, lost
	// leadership). gRPC: Unavailable. HTTP: 503.
	KindUnavailable

	// KindUnauthenticated: the caller is not authenticated. gRPC:
	// Unauthenticated. HTTP: 401.
	KindUnauthenticated

	// KindPermissionDenied: the caller is authenticated but lacks the right
	// to perform this action. gRPC: PermissionDenied. HTTP: 403.
	KindPermissionDenied

	// KindInternal: an invariant on the server side is broken. The caller
	// cannot fix it. gRPC: Internal. HTTP: 500.
	KindInternal

	// KindResourceExhausted maps to gRPC ResourceExhausted / HTTP 429. Used for
	// limit/quota conditions that will not clear on a fast retry (e.g. disk-full
	// write gate), signalling clients to back off rather than hammer-retry.
	KindResourceExhausted
)

// String returns the canonical name of the kind. Used in tests and logging,
// not part of the wire contract — Reason() is the client-facing identifier.
func (k ErrorKind) String() string {
	switch k {
	case KindValidation:
		return "Validation"
	case KindNotFound:
		return "NotFound"
	case KindAlreadyExists:
		return "AlreadyExists"
	case KindConflict:
		return "Conflict"
	case KindPrecondition:
		return "Precondition"
	case KindUnavailable:
		return "Unavailable"
	case KindUnauthenticated:
		return "Unauthenticated"
	case KindPermissionDenied:
		return "PermissionDenied"
	case KindInternal:
		return "Internal"
	case KindResourceExhausted:
		return "ResourceExhausted"
	default:
		return fmt.Sprintf("ErrorKind(%d)", int(k))
	}
}
