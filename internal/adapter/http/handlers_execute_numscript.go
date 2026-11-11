package http

import (
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleExecuteNumscript executes a script independently of transaction creation.
func (s *Server) handleExecuteNumscript(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	payload := &servicepb.ExecuteNumscriptPayload{}
	if err := json.UnmarshalRead(r.Body, payload); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	logs, err := s.applyRequests(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: ledgerName,
			Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_ExecuteNumscript{ExecuteNumscript: payload}},
		}},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	details := map[string]any{"ledger": ledgerName}
	logEntry := exactlyOneLog("execute-numscript", logs, details)
	ledgerLog := logEntry.GetPayload().GetApply().GetLog()
	switch result := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if result.CreatedTransaction == nil {
			panic(emptyLogPayload("execute-numscript", logEntry, details))
		}
		writeCreated(w, r, result.CreatedTransaction)
	case *commonpb.LedgerLogPayload_AccountMetadataUpdated:
		if result.AccountMetadataUpdated == nil {
			panic(emptyLogPayload("execute-numscript", logEntry, details))
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		panic(unexpectedLogPayload("execute-numscript", logEntry, details))
	}
}
