package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
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

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{
					Data: &ledgerpb.LedgerAction_DeleteMetadata{
						DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
							Target: &ledgerpb.Target{
								Target: &ledgerpb.Target_Account{
									Account: &ledgerpb.TargetAccount{
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
