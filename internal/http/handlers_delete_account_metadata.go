package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleDeleteAccountMetadata handles DELETE /{ledgerName}/accounts/{address}/metadata/{key} to delete account metadata
func (s *Server) handleDeleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
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

	key := chi.URLParam(r, "key")
	if key == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("metadata key is required"))
		return
	}

	params := service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
			Address: address,
			Key:     key,
		},
	}

	ledger, err := s.backend.GetLedger(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	_, err = ledger.DeleteAccountMetadata(r.Context(), params)
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "address": address, "key": key, "error": err}).Errorf("Failed to delete account metadata")
		handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
