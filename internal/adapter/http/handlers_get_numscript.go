package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleGetNumscript handles GET /{ledgerName}/numscripts/{name} to get a numscript.
func (s *Server) handleGetNumscript(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))

		return
	}

	version := r.URL.Query().Get("version") // "" = latest

	info, err := s.backend.GetNumscript(r.Context(), ledgerName, name, version)
	if err != nil {
		// Route every error — including ErrNumscriptNotFound — through
		// handleError so the emitted errorCode stays uniform with the
		// Describable Reason() contract introduced by #432. The hardcoded
		// "NOT_FOUND" branch this handler used to take violated that
		// contract by emitting a different code than every other
		// endpoint for the same domain error type.
		handleError(w, r, err)

		return
	}

	writeOK(w, info)
}
