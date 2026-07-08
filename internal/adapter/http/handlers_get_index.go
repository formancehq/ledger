package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleGetIndex handles GET /{ledgerName}/indexes/{canonicalId} to fetch
// a single index registry entry.
func (s *Server) handleGetIndex(w http.ResponseWriter, r *http.Request) {
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

	idx, err := s.backend.GetIndex(r.Context(), &servicepb.GetIndexRequest{
		Ledger: ledgerName,
		Id:     id,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, idx)
}
