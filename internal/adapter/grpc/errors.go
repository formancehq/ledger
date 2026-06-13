package grpc

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
)

const errorDomain = "ledger"

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
	st := status.New(kindToGRPCCode(d.Kind()), d.Error())

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
