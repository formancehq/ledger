package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleDeleteTransactionMetadata handles DELETE /{ledgerName}/transactions/{transactionId}/metadata/{key} to delete transaction metadata
func (s *Server) handleDeleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
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

	key := chi.URLParam(r, "key")
	if key == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("metadata key is required"))
		return
	}

	params := service.Parameters[*servicepb.DeleteTransactionMetadataRequestPayload]{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input: &servicepb.DeleteTransactionMetadataRequestPayload{
			TransactionId: transactionID,
			Key:           key,
		},
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	_, err = s.backend.DeleteTransactionMetadata(r.Context(), ledgerInfo.Id, params)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"key":            key,
			"error":          err,
		}).Errorf("Failed to delete transaction metadata")
		handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
