package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleDeleteLedgerMetadata handles DELETE /{ledgerName}/metadata/{key} to delete ledger metadata.
func (s *Server) handleDeleteLedgerMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	key := chi.URLParam(r, "key")
	if key == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("metadata key is required"))

		return
	}

	_, err := s.applyUnsigned(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_DeleteLedgerMetadata{
			DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{
				Ledger: ledgerName,
				Key:    key,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
