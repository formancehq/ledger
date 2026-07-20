package http

import (
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// kindToHTTPStatus maps a semantic ErrorKind to an HTTP status code. Adding
// a new Kind without a branch fails the `exhaustive` golangci-lint rule;
// that's the structural guarantee that drives this design (#431).
func kindToHTTPStatus(k domain.ErrorKind) int {
	switch k { //exhaustive:enforce
	case domain.KindValidation:
		return http.StatusBadRequest
	case domain.KindNotFound:
		return http.StatusNotFound
	case domain.KindAlreadyExists:
		return http.StatusConflict
	case domain.KindConflict:
		return http.StatusConflict
	case domain.KindPrecondition:
		return http.StatusBadRequest
	case domain.KindUnavailable:
		return http.StatusServiceUnavailable
	case domain.KindUnauthenticated:
		return http.StatusUnauthorized
	case domain.KindPermissionDenied:
		return http.StatusForbidden
	case domain.KindInternal:
		return http.StatusInternalServerError
	case domain.KindResourceExhausted:
		return http.StatusTooManyRequests
	}

	// Unreachable when the exhaustive linter is enabled.
	return http.StatusInternalServerError
}

// handleError converts a server-side error into a JSON-formatted HTTP
// response. The bulk of the work is delegated to the domain.Describable
// contract: any *Err* type or sentinel from internal/domain flows through
// kindToHTTPStatus + Reason(). The few branches below handle errors that
// are not domain Describables — the leader-discovery sentinel from
// commonpb and the generic NotFoundError used by route lookups.
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	// commonpb.ErrNoLeader carries its own retry hint (Retry-After) — keep
	// the dedicated branch so the response shape stays stable for clients
	// that have been told to honour it.
	if errors.Is(err, commonpb.ErrNoLeader) {
		w.Header().Set("Retry-After", "1")
		writeErrorResponse(w, http.StatusServiceUnavailable, "NO_LEADER", err)

		return
	}

	// plan.ErrCacheHorizonExceeded is an infrastructure-level admission
	// rejection (not a domain business outcome, hence not a Describable):
	// admission predicted 2+ cache rotations between propose and apply so
	// any preload would be discarded before FSM read. Retry against a fresh
	// admission snapshot. Mirrors the gRPC adapter's codes.Unavailable
	// mapping (see internal/adapter/grpc/server.go).
	if errors.Is(err, plan.ErrCacheHorizonExceeded) {
		w.Header().Set("Retry-After", "1")
		writeErrorResponse(w, http.StatusServiceUnavailable, "CACHE_HORIZON_EXCEEDED", err)

		return
	}

	var notFoundErr *commonpb.NotFoundError
	if errors.As(err, &notFoundErr) {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

		return
	}

	// domain.ErrNotFound is a bare sentinel (not a typed NotFoundError and not
	// a Describable), returned by read-side queries when the target ledger or
	// resource is absent — e.g. reading numscripts or prepared-queries against
	// a deleted ledger via query.GetLedgerByName. Without this branch it falls
	// through to the 500 sanitizer; a missing resource on a read is a 404.
	if errors.Is(err, domain.ErrNotFound) {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

		return
	}

	// Domain Describables: every typed *Err* and sentinel in internal/domain
	// (and transitively in admission/numscript) flows through this branch.
	// Catches BusinessError too (it implements Describable transparently).
	var d domain.Describable
	if errors.As(err, &d) {
		writeErrorResponse(w, kindToHTTPStatus(domain.Kind(d)), d.Reason(), err)

		return
	}

	// A gRPC InvalidArgument status is a caller error, not a server fault, and
	// must not degrade to a 500. The audit-filter compiler (query.CompileAuditFilter,
	// shared with the gRPC surface) rejects filters that parse but are not
	// audit-supported — `not outcome == failure`, a non-audit condition, an unknown
	// field — as codes.InvalidArgument; surface that as a 400. Only
	// InvalidArgument is translated here; other status codes keep the generic
	// 500 fallthrough deliberately, since no other HTTP handler is expected to
	// produce them and we do not want to leak arbitrary gRPC semantics.
	if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New(st.Message()))

		return
	}

	writeInternalServerError(w, r, err)
}
