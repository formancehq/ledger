package http

import (
	"net/http"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// handlePromoteLedger handles POST /{ledgerName}/promote to promote a mirror ledger to normal mode.
func (s *Server) handlePromoteLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &ledgerpb.Request{
		Type: &ledgerpb.Request_PromoteLedger{
			PromoteLedger: &ledgerpb.PromoteLedgerRequest{
				Ledger: ledgerName,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	details := map[string]any{"ledger": ledgerName}

	logEntry := exactlyOneLog("promote-ledger", logs, details)

	promoteLedgerLog := logEntry.GetPayload().GetPromoteLedger()
	if promoteLedgerLog == nil {
		panic(unexpectedLogPayload("promote-ledger", logEntry, details))
	}

	writeCreated(w, &ledgerpb.LedgerInfo{Name: promoteLedgerLog.GetName(), Mode: ledgerpb.LedgerMode_LEDGER_MODE_NORMAL})
}
