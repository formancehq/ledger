package http

import (
	"net/http"
)

// handleGetEventsSinks handles GET /_/events-sinks to list configured event
// sinks together with their per-sink status (error status + last-emitted
// cursor). The response shape is {sinks, sinkStatuses}.
//
// It is NOT at parity with the gRPC GetEventsSinks RPC any more: the DTOs omit
// every secret-bearing sink field, which the RPC still returns. See
// dto_sinks.go for the list and the rule.
func (s *Server) handleGetEventsSinks(w http.ResponseWriter, r *http.Request) {
	sinks, statuses, err := s.backend.GetEventsSinks(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOKChecked(w, r, newEventsSinksDTO(sinks, statuses))
}
