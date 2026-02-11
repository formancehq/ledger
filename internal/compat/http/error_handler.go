package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
)

// handleError handles errors and returns appropriate HTTP responses
// If the error is ErrNoLeader, it returns 503 Service Unavailable with Retry-After header
// If the error is NotFoundError, it returns 404 Not Found
func handleError(w http.ResponseWriter, r *http.Request, err error) {
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
	if errors.Is(err, processing.ErrTransactionReferenceConflict) {
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)
		return
	}
	if errors.Is(err, processing.ErrIdempotencyKeyConflict) {
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)
		return
	}
	writeInternalServerError(w, r, err)
}
