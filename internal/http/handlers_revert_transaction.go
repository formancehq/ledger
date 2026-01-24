package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleRevertTransaction handles POST /{ledgerName}/transactions/{transactionId}/revert to revert a transaction
func (s *Server) handleRevertTransaction(w http.ResponseWriter, r *http.Request) {
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

	// Decode request body (optional - can be empty or contain metadata, force, atEffectiveDate)
	var reqBody map[string]interface{}
	if r.ContentLength > 0 {
		if err := json.UnmarshalRead(r.Body, &reqBody); err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
			return
		}
	}

	// Build request payload
	payload := &ledgerpb.RevertTransactionRequestPayload{
		TransactionId: transactionID,
	}

	// Extract optional fields from request body
	if reqBody != nil {
		if metadata, ok := reqBody["metadata"].(map[string]interface{}); ok {
			payload.Metadata = make(map[string]string)
			for k, v := range metadata {
				if strVal, ok := v.(string); ok {
					payload.Metadata[k] = strVal
				}
			}
		}
		if force, ok := reqBody["force"].(bool); ok {
			payload.Force = force
		}
		if atEffectiveDate, ok := reqBody["atEffectiveDate"].(bool); ok {
			payload.AtEffectiveDate = atEffectiveDate
		}
	}

	params := service.Parameters[*ledgerpb.RevertTransactionRequestPayload]{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input:          payload,
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	log, err := s.backend.RevertTransaction(r.Context(), ledgerInfo.Id, params)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"error":          err,
		}).Errorf("Failed to revert transaction")
		handleError(w, r, err)
		return
	}

	// Return the revert transaction response
	revertedPayload := log.Data.Payload.(*ledgerpb.LogPayload_RevertedTransaction).RevertedTransaction.RevertTransaction
	writeCreated(w, revertedPayload)
}
