package grpc

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
)

const errorDomain = "ledger"

// validationError is a transport-layer validation Describable for request
// guards whose vocabulary is gRPC-specific (e.g. "envelope") and therefore
// must not live in the domain layer. It mirrors domain.validationSentinel —
// Kind=Validation, Reason=VALIDATION, no per-occurrence metadata — so it
// routes through the same convertToGRPCError path to codes.InvalidArgument
// with a VALIDATION ErrorInfo.
type validationError struct{ msg string }

func (e *validationError) Error() string             { return e.msg }
func (*validationError) Reason() string              { return domain.ErrReasonValidation }
func (*validationError) Metadata() map[string]string { return nil }

// errEnvelopesRequired guards Apply against an empty batch. "Envelope" is a
// servicepb transport carrier (a signed/unsigned request wrapper), not a
// domain concept, so this sentinel lives in the adapter layer (EN-1253
// review).
var errEnvelopesRequired = &validationError{msg: "at least one envelope is required"}

// kindToGRPCCode maps a semantic ErrorKind to a gRPC status code. Adding a
// new Kind without a branch fails the `exhaustive` golangci-lint rule, which
// is the whole point of this design (#431): a new domain error cannot reach
// the API without a declared mapping.
func kindToGRPCCode(k domain.ErrorKind) codes.Code {
	switch k { //exhaustive:enforce
	case domain.KindValidation:
		return codes.InvalidArgument
	case domain.KindNotFound:
		return codes.NotFound
	case domain.KindAlreadyExists:
		return codes.AlreadyExists
	case domain.KindConflict:
		return codes.FailedPrecondition
	case domain.KindPrecondition:
		return codes.FailedPrecondition
	case domain.KindUnavailable:
		return codes.Unavailable
	case domain.KindUnauthenticated:
		return codes.Unauthenticated
	case domain.KindPermissionDenied:
		return codes.PermissionDenied
	case domain.KindInternal:
		return codes.Internal
	}

	// Unreachable: every Kind defined in domain has a branch above, and
	// adding a new one without updating this switch fails CI.
	return codes.Internal
}

// describableToGRPCStatus converts a Describable to a gRPC status with the
// ErrorInfo detail clients pattern-match on. The Kind selects the status
// code via the exhaustive switch above; the Reason and Metadata carry the
// per-type wire contract.
func describableToGRPCStatus(d domain.Describable) *status.Status {
	st := status.New(kindToGRPCCode(domain.Kind(d)), d.Error())

	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   d.Reason(),
		Domain:   errorDomain,
		Metadata: d.Metadata(),
	})
	if err != nil {
		return st
	}

	return detailed
}

// businessErrorToGRPCStatus is the thin shim still consumed by tests and
// (transitively) by convertToGRPCError in server.go. New code should call
// describableToGRPCStatus directly with a Describable.
func businessErrorToGRPCStatus(bizErr *domain.BusinessError) *status.Status {
	return describableToGRPCStatus(bizErr.Err)
}
