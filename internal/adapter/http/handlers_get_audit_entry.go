package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// handleGetAuditEntry handles GET /v3/_/audit-entries/{sequence}.
//
// Returns a single audit entry by its global sequence, with per-order items
// populated (mirroring gRPC BucketService.GetAuditEntry). A missing sequence
// maps to 404 via the controller's NotFoundError.
func (s *Server) handleGetAuditEntry(w http.ResponseWriter, r *http.Request) {
	sequence, ok := requireAuditSequence(w, r)
	if !ok {
		return
	}

	entry, err := s.backend.GetAuditEntry(r.Context(), sequence)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, entry)
}

// requireAuditSequence extracts and validates the {sequence} URL parameter.
func requireAuditSequence(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	raw := chi.URLParam(r, "sequence")
	if raw == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("sequence is required"))

		return 0, false
	}

	sequence, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid sequence: %w", err))

		return 0, false
	}

	return sequence, true
}
