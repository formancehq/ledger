package http

import (
	"net/http"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleDeleteAccountMetadata handles DELETE /{ledgerName}/accounts/{address}/metadata/{key} to delete account metadata.
func (s *Server) handleDeleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	address, ok := requirePathParameter(w, r, "address", "account address")
	if !ok {
		return
	}

	key, ok := requireMetadataKey(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_DeleteMetadata{
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
