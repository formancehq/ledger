package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleGetNumscriptUsage handles GET /{ledgerName}/numscripts/{name}/usage.
// Returns the invocation counter + last-used timestamp populated by the
// usagebuilder subsystem. Values are eventually consistent with the FSM
// (may lag by up to one usagebuilder tick). A never-invoked template
// returns a zero-valued response, not a 404 — clients treat 0 uniformly.
func (s *Server) handleGetNumscriptUsage(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))

		return
	}

	usage, err := s.backend.GetTemplateUsage(r.Context(), ledgerName, name)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, usage)
}
