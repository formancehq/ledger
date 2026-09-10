package grpc

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
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

// describableToGRPCStatus converts a Describable to a gRPC status with the
// ErrorInfo detail clients pattern-match on. The Kind selects the status code
// through grpcerr.CodeForKind — the one encode table, shared with the decoder
// that reverses it, so the two directions cannot drift; the Reason and the
// type-owned public presentation carry the wire contract without exposing
// diagnostic context.
func describableToGRPCStatus(d domain.Describable) *status.Status {
	message, metadata, _ := domain.PublicErrorDetails(d)
	st := status.New(grpcerr.CodeForKind(domain.Kind(d)), message)

	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   d.Reason(),
		Domain:   errorDomain,
		Metadata: metadata,
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
