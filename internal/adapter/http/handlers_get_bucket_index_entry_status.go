package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleGetBucketIndexEntryStatus handles GET /indexes/{canonicalId}/status
// to fetch the per-replica status view of a bucket-scoped index.
func (s *Server) handleGetBucketIndexEntryStatus(w http.ResponseWriter, r *http.Request) {
	canonical := chi.URLParam(r, "canonicalId")
	if canonical == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("index id is required"))

		return
	}

	id, err := indexes.ParseCanonical(canonical)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	entry, err := s.backend.GetIndexEntryStatus(r.Context(), &servicepb.GetIndexEntryStatusRequest{
		Id: id,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, entry)
}
