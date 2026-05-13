package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleDeleteAccountMetadata handles DELETE /{ledgerName}/accounts/{address}/metadata/{key} to delete account metadata.
func (s *Server) handleDeleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
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

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_DeleteMetadata{
						DeleteMetadata: &commonpb.DeleteMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{
										Addr: address,
									},
								},
							},
							Key: key,
						},
					},
				},
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "address": address, "key": key, "error": err}).Errorf("Failed to delete account metadata")
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
