package http

import (
	"net/http"
)

// handleListNumscripts handles GET /numscripts to list all numscripts.
func (s *Server) handleListNumscripts(w http.ResponseWriter, r *http.Request) {
	scripts, err := s.backend.ListNumscripts(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, scripts)
}
