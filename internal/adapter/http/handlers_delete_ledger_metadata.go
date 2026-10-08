package http

import (
	"net/http"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
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

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &ledgerpb.Request{
		Type: &ledgerpb.Request_DeleteLedgerMetadata{
			DeleteLedgerMetadata: &ledgerpb.DeleteLedgerMetadataRequest{
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
