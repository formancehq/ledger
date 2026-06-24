package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/domain"
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

	var notFoundErr *commonpb.NotFoundError
	if errors.As(err, &notFoundErr) {
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

	writeInternalServerError(w, r, err)
}
