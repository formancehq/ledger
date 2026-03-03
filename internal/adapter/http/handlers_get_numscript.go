package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/go-chi/chi/v5"
)

// handleGetNumscript handles GET /numscripts/{name} to get a numscript.
func (s *Server) handleGetNumscript(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))
		return
	}

	version := r.URL.Query().Get("version") // "" = latest

	info, err := s.backend.GetNumscript(r.Context(), name, version)
	if err != nil {
		var notFound *domain.ErrNumscriptNotFound
		if errors.As(err, &notFound) {
			writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)
			return
		}
		handleError(w, r, err)
		return
	}

	writeOK(w, info)
}
