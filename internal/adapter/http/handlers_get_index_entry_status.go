package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleGetIndexEntryStatus handles GET /{ledgerName}/indexes/{canonicalId}/status
// to fetch the per-replica status view (registry entry + backfill cursor +
// IndexVersionState) for a single index.
func (s *Server) handleGetIndexEntryStatus(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

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
		Ledger: ledgerName,
		Id:     id,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, entry)
}
