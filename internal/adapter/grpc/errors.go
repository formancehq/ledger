package grpc

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
)

const errorDomain = "ledger"

// validationError is a transport-layer validation error for request guards
// whose vocabulary is gRPC-specific (e.g. "envelope") and therefore must not
// live in the domain layer. It is a domain.Classifiable and nothing more.
//
// It used to declare the generic VALIDATION reason, which told a client
// strictly nothing its InvalidArgument status did not already say, while
// committing the server to a wire identifier it could never rename. A
// transport guard has no business outcome to name: the status code is the
// whole contract.
type validationError struct{ msg string }

func (e *validationError) Error() string        { return e.msg }
func (*validationError) Kind() domain.ErrorKind { return domain.KindValidation }

// errEnvelopesRequired guards Apply against an empty batch. "Envelope" is a
// servicepb transport carrier (a signed/unsigned request wrapper), not a
// domain concept, so this sentinel lives in the adapter layer (EN-1253
// review).
var errEnvelopesRequired = &validationError{msg: "at least one envelope is required"}

// classifiableToGRPCStatus converts any classified error to a gRPC status. The
// Kind alone selects the status code, through grpcerr.CodeForKind — the one
// encode table, shared with the decoder that reverses it, so the two directions
// cannot drift.
//
// The ErrorInfo detail is attached only when the error also owns a public wire
// contract. An error that is merely Classifiable has no stable Reason for a
// client to match, so inventing one here would ship a wire identifier the type
// never committed to; it reaches the client as the right code and message with
// no ErrorInfo.
func classifiableToGRPCStatus(c domain.Classifiable) *status.Status {
	code := grpcerr.CodeForKind(c.Kind())

	d, ok := c.(domain.Describable)
	if !ok {
		return status.New(code, c.Error())
	}

	return describableToGRPCStatus(code, d)
}

// describableToGRPCStatus adds the ErrorInfo detail clients pattern-match on:
// the Reason and the type-owned public presentation carry the wire contract
// without exposing diagnostic context.
func describableToGRPCStatus(code codes.Code, d domain.Describable) *status.Status {
	message, metadata, _ := domain.PublicErrorDetails(d)
	st := status.New(code, message)

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

// businessErrorToGRPCStatus is the thin shim still consumed by tests. New code
// should call classifiableToGRPCStatus, the pipeline entry point.
func businessErrorToGRPCStatus(bizErr *domain.BusinessError) *status.Status {
	return classifiableToGRPCStatus(bizErr.Err)
}
