package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// handleGetLog handles GET /logs/{sequence} to fetch a single system log by
// its bucket-wide sequence number.
func (s *Server) handleGetLog(w http.ResponseWriter, r *http.Request) {
	seqRaw := chi.URLParam(r, "sequence")

	sequence, err := strconv.ParseUint(seqRaw, 10, 64)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid sequence parameter"))

		return
	}

	log, err := s.backend.GetLog(r.Context(), sequence)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, log)
}
