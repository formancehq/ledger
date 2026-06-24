package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handlePromoteLedger handles POST /{ledgerName}/promote to promote a mirror ledger to normal mode.
func (s *Server) handlePromoteLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
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

	if len(logs) == 0 {
		unreachable("promote-ledger apply returned no log", map[string]any{"ledger": ledgerName})
	}

	promoteLedgerLog := logs[0].GetPayload().GetPromoteLedger()
	if promoteLedgerLog == nil {
		writeInternalServerError(w, r, errors.New("unexpected log payload type"))

		return
	}

	writeCreated(w, &commonpb.LedgerInfo{Name: promoteLedgerLog.GetName(), Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL})
}
