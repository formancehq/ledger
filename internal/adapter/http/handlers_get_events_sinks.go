package http

import (
	"net/http"
)

// handleGetEventsSinks handles GET /events-sinks to list configured event
// sinks.
func (s *Server) handleGetEventsSinks(w http.ResponseWriter, r *http.Request) {
	sinks, err := s.backend.GetEventsSinks(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, sinks)
}
