package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// handleSaveAccountMetadata handles POST /{ledgerName}/accounts/{address}/metadata to save account metadata.
func (s *Server) handleSaveAccountMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	address := chi.URLParam(r, "address")
	if address == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("account address is required"))

		return
	}

	ms, ok := parseMetadataBody(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{
					Data: &ledgerpb.LedgerAction_AddMetadata{
						AddMetadata: &ledgerpb.SaveMetadataCommand{
							Target: &ledgerpb.Target{
								Target: &ledgerpb.Target_Account{
									Account: &ledgerpb.TargetAccount{
										Addr: address,
									},
								},
							},
							Metadata: ms,
						},
					},
				},
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "address": address, "error": err}).Errorf("Failed to save account metadata")
		handleError(w, r, err)

		return
	}

	// Return 204 No Content (no Content-Type header for 204)
	w.WriteHeader(http.StatusNoContent)
}
