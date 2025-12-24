package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// handleError handles errors and returns appropriate HTTP responses
// If the error is ErrNoLeader, it returns 503 Service Unavailable with Retry-After header
// If the error is NotFoundError, it returns 404 Not Found
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, ledgerpb.ErrNoLeader) {
		w.Header().Set("Retry-After", "1")
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "NO_LEADER", err)
		return
	}
	var notFoundErr *ledgerpb.NotFoundError
	if errors.As(err, &notFoundErr) {
		api.WriteErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)
		return
	}
	api.InternalServerError(w, r, err)
}
