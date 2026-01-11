package http

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleSaveTransactionMetadata handles POST /{ledgerName}/transactions/{transactionId}/metadata to save transaction metadata
func (s *Server) handleSaveTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	transactionIDRaw := chi.URLParam(r, "transactionId")
	if transactionIDRaw == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("transaction id is required"))
		return
	}

	transactionID, err := strconv.ParseUint(transactionIDRaw, 10, 64)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid transaction id: %w", err))
		return
	}

	var inputMetadata map[string]string
	if err := json.UnmarshalRead(r.Body, &inputMetadata); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	params := service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
			TransactionId: transactionID,
			Metadata:      inputMetadata,
		},
	}

	ledger, err := s.backend.GetLedger(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	_, err = ledger.SaveTransactionMetadata(r.Context(), params)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"error":          err,
		}).Errorf("Failed to save transaction metadata")
		handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
