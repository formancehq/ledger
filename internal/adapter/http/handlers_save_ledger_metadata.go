package http

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_SaveLedgerMetadata{
			SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{
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
