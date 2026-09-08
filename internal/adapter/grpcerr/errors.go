// Package grpcerr reconstructs typed domain errors from gRPC statuses
// arriving off the network.
//
// The server serialises a domain.Describable faithfully: the Kind selects the
// status code and the reason plus metadata ride along in an errdetails.ErrorInfo
// (see internal/adapter/grpc.describableToGRPCStatus). A client receiving that
// status holds a *status.Error, which is not a Describable, so every consumer
// that dispatches on the Describable contract — the HTTP error handler, the
// bulk per-element mapper, the CLI formatter — falls through to its generic
// "unknown error" branch. This package turns the wire representation back into
// a Describable so those consumers keep working across a network hop.
//
// It is deliberately a leaf: it imports only gRPC, errdetails, internal/domain
// and internal/proto/commonpb. internal/adapter/grpc, the intuitive home,
// transitively pulls in the Pebble DAL, the readstore, the usage store, backup
// and the Raft node, which cmd/ledgerctl must not link.
package grpcerr

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// errorDomain is the ErrorInfo.domain the server stamps on every business
// error. Details carrying any other domain belong to another service and are
// left alone.
const errorDomain = "ledger"

// FromStatusError turns a gRPC status error into a typed error whose chain
// satisfies the contract its consumers dispatch on, and returns every other
// error unchanged.
//
// Two shapes are reconstructed:
//
//   - A status carrying a ledger-domain ErrorInfo becomes a
//     *domain.BusinessError wrapping a *domain.RemoteError, so
//     errors.AsType[domain.Describable] succeeds and domain.Kind yields the
//     kind the wire described.
//   - A bare codes.NotFound (no ErrorInfo — roughly twenty
//     commonpb.NewNotFoundError sites) becomes a *commonpb.NotFoundError,
//     which the HTTP handler already maps to 404.
//
// Everything else is returned as-is, by design:
//
//   - codes.Canceled must stay raw. internal/adapter/grpc/cursor.go keys
//     end-of-stream detection off status.Code(err) == codes.Canceled, and
//     normalises it to io.EOF; reconstructing it would break pagination.
//   - A bare codes.Unavailable already reaches the right outcome. The HTTP
//     handler maps that code to 503 + Retry-After on its own, and the class
//     (no leader yet, peer missing from the pool, stream torn down) has no
//     reason code to recover.
//   - Internal, Unknown and DeadlineExceeded are server faults or transport
//     conditions with nothing typed to restore.
//
// The returned error still answers GRPCStatus() with the original status, so
// status.Code and status.FromError keep working for callers that read the code
// directly, and convertToGRPCError re-derives the same status when the value
// crosses a second hop.
func FromStatusError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status: a local failure, io.EOF from a stream end, or a
		// plain wrapped error. Nothing to reconstruct.
		return err
	}

	if st.Code() == codes.OK || st.Code() == codes.Canceled {
		return err
	}

	if bizErr := BusinessErrorFromGRPC(err); bizErr != nil {
		return &reconstructedError{st: st, inner: bizErr}
	}

	if st.Code() == codes.NotFound {
		return &reconstructedError{st: st, inner: commonpb.NewNotFoundError("%s", st.Message())}
	}

	return err
}

// reconstructedError carries a typed error recovered from the wire while
// remaining a gRPC status error.
//
// It deliberately does NOT implement domain.Describable. errors.AsType walks
// the chain, so a consumer looking for a Describable unwraps past this type to
// the *domain.BusinessError inside, and domain.Kind then reaches
// RemoteError.kindOverride and returns the kind the wire carried. Were this
// type to implement Describable itself (by embedding, say), domain.Kind would
// match neither the kindOverride branch nor the *BusinessError branch: it would
// fall through to KindForReason and silently discard the override that exists
// precisely to survive an unknown reason.
type reconstructedError struct {
	st    *status.Status
	inner error
}

func (e *reconstructedError) Error() string { return e.inner.Error() }

// Unwrap exposes the reconstructed typed error to errors.Is/As/AsType.
func (e *reconstructedError) Unwrap() error { return e.inner }

// GRPCStatus keeps the original status reachable through status.FromError, so
// the code survives reconstruction. cursor.go's end-of-stream check and
// convertToGRPCError's "already a status, return as-is" shortcut both depend
// on it.
func (e *reconstructedError) GRPCStatus() *status.Status { return e.st }

// BusinessErrorFromGRPC extracts a BusinessError from a gRPC status error.
// Returns nil if the error is not a business error (no ErrorInfo with
// domain "ledger"). The returned BusinessError.Err is a *domain.RemoteError
// transporting the wire contract (Reason, Metadata, Message) plus the Kind
// derived from the reason, falling back to the status code. New server-side
// error types reach this code path automatically — no client-side switch to
// extend.
func BusinessErrorFromGRPC(err error) *domain.BusinessError {
	st := status.Convert(err)
	if st.Code() == codes.OK {
		return nil
	}

	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != errorDomain || info.GetReason() == "" {
			continue
		}

		return &domain.BusinessError{
			Err: &domain.RemoteError{
				KindValue:   kindForWire(info.GetReason(), st.Code()),
				ReasonValue: info.GetReason(),
				Message:     st.Message(),
				Meta:        info.GetMetadata(),
			},
		}
	}

	return nil
}

// kindForWire derives the ErrorKind of a received error from the reason first
// and the status code only as a fallback.
//
// Neither source is sufficient alone. Reason-only is unsound across versions:
// a client older than the server receives a reason its enum does not know,
// ReasonCode yields UNSPECIFIED, and KindForReason collapses it to KindInternal
// — turning a caller mistake into a 500. Code-only loses information the wire
// carried: kindToGRPCCode maps both KindConflict and KindPrecondition to
// codes.FailedPrecondition, so deriving from the code alone reports every
// KindConflict reason (LEDGER_DELETED, TRANSACTION_ALREADY_REVERTED,
// LEDGER_IN_MIRROR_MODE, ACCOUNT_TYPE_HAS_ACCOUNTS, ACCOUNT_TYPE_CONFLICT,
// STALE_CLUSTER_POLICY) as KindPrecondition and answers 400 where the leader
// answers 409.
//
// Reason-first gets both: a reason this build knows is authoritative, and an
// unknown one still keeps whatever the status code described.
func kindForWire(reason string, code codes.Code) domain.ErrorKind {
	if rc := domain.ReasonCode(reason); rc != commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return domain.KindForReason(rc)
	}

	return kindForCode(code)
}

// kindForCode reverses the server-side kindToGRPCCode mapping. It is the
// fallback for a reason this build does not know, so the collapse of
// KindConflict and KindPrecondition onto codes.FailedPrecondition is
// unavoidable here; kindForWire resolves it from the reason whenever it can.
func kindForCode(c codes.Code) domain.ErrorKind {
	switch c {
	case codes.InvalidArgument:
		return domain.KindValidation
	case codes.NotFound:
		return domain.KindNotFound
	case codes.AlreadyExists:
		return domain.KindAlreadyExists
	case codes.FailedPrecondition:
		return domain.KindPrecondition
	case codes.ResourceExhausted:
		return domain.KindResourceExhausted
	case codes.Unavailable:
		return domain.KindUnavailable
	case codes.Unauthenticated:
		return domain.KindUnauthenticated
	case codes.PermissionDenied:
		return domain.KindPermissionDenied
	default:
		return domain.KindInternal
	}
}
