package http

import (
	"net/http"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// handleDeleteLedger handles DELETE /{ledgerName} to delete a ledger.
func (s *Server) handleDeleteLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &ledgerpb.Request{
		Type: &ledgerpb.Request_DeleteLedger{
			DeleteLedger: &ledgerpb.DeleteLedgerRequest{
				Name: ledgerName,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	// Return 204 No Content on successful deletion
	w.WriteHeader(http.StatusNoContent)
}
