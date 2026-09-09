// Package grpcerr is the gRPC decoder for the internal/adapter/apierr boundary
// contract, and the owner of the single ErrorKind-to-status-code table.
//
// The server serialises a domain.Describable faithfully: CodeForKind selects
// the status code and the reason plus metadata ride along in an
// errdetails.ErrorInfo (see internal/adapter/grpc.describableToGRPCStatus,
// which encodes through the table in this package). A receiver holds a
// *status.Error, which carries no semantic classification at all, so every
// consumer that dispatches on the boundary contract — the HTTP error handler,
// the bulk per-element mapper, the CLI formatter — falls through to its
// generic "unknown error" branch. This package turns the wire representation
// back into an *apierr.Remote so those consumers keep working across a network
// hop.
//
// FromStatusError reconstructs exactly two shapes and returns everything else
// unchanged; read the rule as that complement rather than as a list of
// excluded codes.
//
//   - A status carrying a ledger-domain ErrorInfo becomes an *apierr.Remote —
//     whatever its code. A KindUnavailable or KindInternal Describable arrives
//     as codes.Unavailable or codes.Internal *with* an ErrorInfo, and is
//     reconstructed like any other, so INDEX_BUILDING and COVERAGE_MISS keep
//     their reason across the hop.
//   - A bare codes.NotFound (no ErrorInfo — roughly twenty
//     commonpb.NewNotFoundError sites) becomes a *commonpb.NotFoundError,
//     which the HTTP handler already maps to 404.
//
// Everything else passes through, which covers three groups:
//
//   - codes.Canceled, unconditionally and before the ErrorInfo check.
//     internal/adapter/grpc/cursor.go keys end-of-stream detection off
//     status.Code(err) == codes.Canceled and normalises it to io.EOF;
//     reconstructing it would break pagination.
//   - A *bare* status of any code — no ledger ErrorInfo to decode, and no
//     reason to recover. Bare codes.Unavailable (no leader yet, peer missing
//     from the pool, stream torn down) already reaches the right outcome:
//     handleError maps that code to 503 + Retry-After on its own. Bare
//     Internal, Unknown and DeadlineExceeded are server faults or transport
//     conditions. Bare codes.Unauthenticated and codes.PermissionDenied from
//     the leader denote a cluster-secret failure between nodes rather than a
//     caller credential problem, and correctly stay a 500 rather than
//     surfacing as 401/403 to the caller.
//   - An ErrorInfo stamped with another service's domain, left for that
//     service's client.
//
// A reconstructed error still answers GRPCStatus() with the original status,
// so status.Code and status.FromError keep working for callers that read the
// code directly and convertToGRPCError re-derives the same status when the
// value crosses a second hop. That is what keeps the two axes independent, and
// the transport axis is the lossy one: CodeForKind sends both KindConflict and
// KindPrecondition as codes.FailedPrecondition, so the received code alone
// cannot name the kind. Preserving the status verbatim rather than rebuilding
// it from the kind keeps every code-reading caller correct.
//
// It is deliberately a leaf: it imports only gRPC, errdetails,
// internal/adapter/apierr, internal/domain and internal/proto/commonpb.
// internal/adapter/grpc, the intuitive home, transitively pulls in the Pebble
// DAL, the readstore, the usage store, backup and the Raft node, which
// cmd/ledgerctl must not link — so the shared table lives here and that
// package imports it.
package grpcerr

import (
	"errors"
	"slices"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// errorDomain is the ErrorInfo.domain the server stamps on every business
// error. Details carrying any other domain belong to another service and are
// left alone.
const errorDomain = "ledger"

// CodeForKind maps a semantic ErrorKind to the gRPC status code the server
// sends it under. Adding a new Kind without a branch fails the `exhaustive`
// golangci-lint rule, which is the whole point of this design (#431): a new
// domain error cannot reach the API without a declared mapping.
//
// This is the one encode table. internal/adapter/grpc encodes through it,
// kindForCode reverses it, and allowedWireCodes derives the validation policy
// from it — so the encode and decode directions cannot drift apart.
func CodeForKind(k domain.ErrorKind) codes.Code {
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
	case domain.KindResourceExhausted:
		return codes.ResourceExhausted
	}

	// Unreachable: every Kind defined in domain has a branch above, and
	// adding a new one without updating this switch fails CI.
	return codes.Internal
}

// allowedWireCodes returns every status code a ledger server may legitimately
// send rc under.
//
// Today that is exactly one code. Every reason in the enum reaches the wire
// through describableToGRPCStatus (internal/adapter/grpc/errors.go), which
// derives the status from CodeForKind and nothing else, so a second legitimate
// code for a known reason cannot arise without a deliberate encoder change.
// The three reasons the server hand-builds an ErrorInfo for
// (EXTERNAL_SERVICE_ERROR, RAFT_NODE_NOT_IN_CLUSTER,
// RAFT_NODE_REMOVAL_COMMITTED) are not enum members, so they decode down the
// unknown-reason path and are never validated here.
//
// Should the transport and semantic axes have to disagree for some reason —
// as they did for the removed READ_INDEX_NOT_CAUGHT_UP, sent as
// codes.FailedPrecondition so callers skipped actions.GRPCRetryPolicy's fifty
// Unavailable retries — widen the returned set for that reason rather than
// relaxing the kind mapping, which the encoder shares.
func allowedWireCodes(rc commonpb.ErrorReason) []codes.Code {
	return []codes.Code{CodeForKind(domain.KindForReason(rc))}
}

// FromStatusError turns a gRPC status error into a typed error whose chain
// satisfies the apierr boundary contract, and returns every other error
// unchanged. See the package documentation for which shapes are reconstructed
// and why the rest are not.
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

	decoded := Decode(err)

	if remote, ok := errors.AsType[*apierr.Remote](decoded); ok {
		return &reconstructedError{st: st, inner: remote}
	}

	if invalid, ok := errors.AsType[*apierr.InvalidWireError](decoded); ok {
		// A protocol fault, not a business outcome. Return it bare: without a
		// GRPCStatus it cannot be answered as the code it arrived under, so it
		// reaches the internal-error sanitizer on every surface and the
		// untrusted reason, message and metadata are never echoed.
		return invalid
	}

	if st.Code() == codes.NotFound {
		return &reconstructedError{st: st, inner: commonpb.NewNotFoundError("%s", st.Message())}
	}

	return err
}

// reconstructedError carries a typed error recovered from the wire while
// remaining a gRPC status error.
//
// It deliberately does NOT implement domain.Describable or expose the decoded
// kind itself. errors.AsType walks the chain, so apierr.Describe unwraps past
// this type to the *apierr.Remote inside and reads the classification the wire
// carried. Were this type to satisfy the contract itself (by embedding, say),
// a consumer would stop at the wrapper and lose the kind — which for a reason
// this build does not know is the whole point of decoding.
type reconstructedError struct {
	st    *status.Status
	inner error
}

func (e *reconstructedError) Error() string { return e.inner.Error() }

// Unwrap exposes the reconstructed typed error to errors.Is/As/AsType.
func (e *reconstructedError) Unwrap() error { return e.inner }

// GRPCStatus keeps the original status reachable through status.FromError, so
// the code survives reconstruction unchanged — the transport axis is preserved
// exactly as received, independently of the semantic kind. cursor.go's
// end-of-stream check and convertToGRPCError's "already a status, return
// as-is" shortcut both depend on it.
func (e *reconstructedError) GRPCStatus() *status.Status { return e.st }

// Decode reads the boundary view out of a gRPC status error. It returns:
//
//   - an *apierr.Remote when the status carries a ledger-domain ErrorInfo
//     whose reason and code are a pair this build's server could have sent;
//   - an *apierr.InvalidWireError when the reason is one this build knows but
//     the code contradicts it;
//   - nil when there is no ledger-domain ErrorInfo to decode.
//
// New server-side error types reach this code path automatically — there is no
// client-side type switch to extend.
func Decode(err error) error {
	st := status.Convert(err)
	if st.Code() == codes.OK {
		return nil
	}

	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != errorDomain || info.GetReason() == "" {
			continue
		}

		return decodeReason(info, st)
	}

	return nil
}

// decodeReason classifies one ledger-domain ErrorInfo.
//
// Kind derivation is reason-first, and neither source is sufficient alone.
// Reason-only is unsound across versions: a receiver older than the sender
// gets a reason its enum does not know, ReasonCode yields UNSPECIFIED, and
// KindForReason collapses it to KindInternal — turning a caller mistake into a
// 500. Code-only loses information the wire carried: CodeForKind maps both
// KindConflict and KindPrecondition to codes.FailedPrecondition, so deriving
// from the code alone reports every KindConflict reason (LEDGER_DELETED,
// TRANSACTION_ALREADY_REVERTED, LEDGER_IN_MIRROR_MODE,
// ACCOUNT_TYPE_HAS_ACCOUNTS, ACCOUNT_TYPE_CONFLICT, STALE_CLUSTER_POLICY) as
// KindPrecondition and answers 400 where the sender answered 409.
//
// A known reason is also validated against the code it arrived under, because
// a reason this build knows is a reason whose legitimate codes it knows too. A
// contradiction there is a protocol fault and nothing received is trusted.
// An unknown reason cannot be validated — this build has no policy for it — so
// its exact code, reason, message and metadata are preserved verbatim and the
// code supplies the classification.
func decodeReason(info *errdetails.ErrorInfo, st *status.Status) error {
	rc := domain.ReasonCode(info.GetReason())
	if rc == commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return &apierr.Remote{
			KindValue:   kindForCode(st.Code()),
			ReasonValue: info.GetReason(),
			Msg:         st.Message(),
			Meta:        info.GetMetadata(),
		}
	}

	if allowed := allowedWireCodes(rc); !slices.Contains(allowed, st.Code()) {
		return &apierr.InvalidWireError{
			ReasonValue: info.GetReason(),
			Code:        st.Code(),
			Expected:    allowed,
		}
	}

	return &apierr.Remote{
		KindValue:   domain.KindForReason(rc),
		ReasonValue: info.GetReason(),
		Msg:         st.Message(),
		Meta:        info.GetMetadata(),
	}
}

// kindForCode reverses CodeForKind. It is the classification for a reason this
// build does not know, so the collapse of KindConflict and KindPrecondition
// onto codes.FailedPrecondition is unavoidable here; decodeReason resolves it
// from the reason whenever it can.
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
