package http

import (
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleSaveLedgerMetadata handles POST /{ledgerName}/metadata to save ledger metadata.
func (s *Server) handleSaveLedgerMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	var inputMetadata map[string]any
	if err := json.UnmarshalRead(r.Body, &inputMetadata); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	ms, err := commonpb.MetadataFromAnyMap(inputMetadata)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid metadata: %w", err))

		return
	}

	_, err = s.backend.Apply(r.Context(), &servicepb.Request{
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
