package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleDeleteLedgerMetadata handles DELETE /{ledgerName}/metadata/{key} to delete ledger metadata.
func (s *Server) handleDeleteLedgerMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	key, ok := requireMetadataKey(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
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
