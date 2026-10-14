package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleSaveAccountMetadata handles POST /{ledgerName}/accounts/{address}/metadata to save account metadata.
func (s *Server) handleSaveAccountMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	address, ok := requirePathParameter(w, r, "address", "account address")
	if !ok {
		return
	}

	ms, ok := parseMetadataBody(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{
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
