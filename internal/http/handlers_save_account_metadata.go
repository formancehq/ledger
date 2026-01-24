package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleSaveAccountMetadata handles POST /{ledgerName}/accounts/{address}/metadata to save account metadata
func (s *Server) handleSaveAccountMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	address := chi.URLParam(r, "address")
	if address == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("account address is required"))
		return
	}

	// Decode request body into metadata
	var inputMetadata map[string]string
	if err := json.UnmarshalRead(r.Body, &inputMetadata); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Build service.Parameters[*ledgerpb.SaveAccountMetadataRequest]
	params := service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input: &ledgerpb.SaveAccountMetadataRequestPayload{
			Address:  address,
			Metadata: &ledgerpb.Metadata{Entries: inputMetadata},
		},
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	_, err = s.backend.SaveAccountMetadata(r.Context(), ledgerInfo.Id, params)
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "address": address, "error": err}).Errorf("Failed to save account metadata")
		handleError(w, r, err)
		return
	}

	// Return 204 No Content (no Content-Type header for 204)
	w.WriteHeader(http.StatusNoContent)
}
