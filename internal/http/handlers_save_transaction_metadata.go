package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	_, err = s.backend.Apply(r.Context(), &servicepb.LedgerAction{
		LedgerId:       ledgerInfo.Id,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Data: &servicepb.LedgerAction_AddMetadata{
			AddMetadata: &commonpb.SaveMetadataCommand{
				Target: &commonpb.Target{
					Target: &commonpb.Target_Transaction{
						Transaction: &commonpb.TargetTransaction{
							Id: transactionID,
						},
					},
				},
				Metadata: &commonpb.Metadata{Entries: inputMetadata},
			},
		},
	})
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
