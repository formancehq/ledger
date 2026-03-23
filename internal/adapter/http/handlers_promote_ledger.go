package http

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handlePromoteLedger handles POST /{ledgerName}/promote to promote a mirror ledger to normal mode.
func (s *Server) handlePromoteLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	logs, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_PromoteLedger{
			PromoteLedger: &servicepb.PromoteLedgerRequest{
				Ledger: ledgerName,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeCreated(w, logs[0].GetPayload().GetPromoteLedger().GetInfo())
}
