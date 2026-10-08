package http

import (
	"net/http"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// handleSaveLedgerMetadata handles POST /{ledgerName}/metadata to save ledger metadata.
func (s *Server) handleSaveLedgerMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	ms, ok := parseMetadataBody(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &ledgerpb.Request{
		Type: &ledgerpb.Request_SaveLedgerMetadata{
			SaveLedgerMetadata: &ledgerpb.SaveLedgerMetadataRequest{
				Ledger:   ledgerName,
				Metadata: ms,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
