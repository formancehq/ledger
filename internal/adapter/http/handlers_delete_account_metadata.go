package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
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

	key, ok := requireMetadataKey(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_DeleteMetadata{
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
