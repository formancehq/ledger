package http

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleDeleteLedger handles DELETE /{ledgerName} to delete a ledger.
func (s *Server) handleDeleteLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
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
