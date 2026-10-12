package http

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleRemoveEventsSink handles DELETE /_/events-sinks/{sinkName}.
// controllerId is the same optional ownership precondition as in gRPC Apply.
func (s *Server) handleRemoveEventsSink(w http.ResponseWriter, r *http.Request) {
	name, ok := requirePathParameter(w, r, "sinkName", "sink name")
	if !ok {
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query["controllerId"]) > 1 {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid controllerId query parameter"))

		return
	}

	_, err = s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_RemoveEventsSink{RemoveEventsSink: &servicepb.RemoveEventsSinkRequest{
			Name: name, ControllerId: query.Get("controllerId"),
		}},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}
