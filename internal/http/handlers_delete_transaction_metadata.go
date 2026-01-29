package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	_, err = s.backend.Apply(r.Context(), &servicepb.Action{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				Ledger: servicepb.LedgerName(ledgerName),
				Data: &servicepb.LedgerApplyAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Id: transactionID,
								},
							},
						},
						Key: key,
					},
				},
			},
		},
	})
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
