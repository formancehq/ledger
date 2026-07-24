package http

import (
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleCreateTransaction handles POST /{ledgerName}/transactions to create a new transaction.
func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	// Decode request body into protobuf CreateTransactionPayload.
	// The custom UnmarshalJSON in service.pb.json.go drives field naming
	// from the public camelCase contract (scriptReference, accountMetadata,
	// …) — the default protoc-gen-go tags are snake_case and would silently
	// drop multi-word keys (#452).
	req := &servicepb.CreateTransactionPayload{}

	err := json.UnmarshalRead(r.Body, req)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	// The unitary endpoint intentionally does NOT expose skippableReasons: a
	// single-transaction caller can catch the 4xx directly. The opt-in lives
	// on the bulk endpoint (per-entry) and on the gRPC LedgerApplyRequest.
	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: req,
					},
				},
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "error": err}).Errorf("Failed to create transaction")
		handleError(w, r, err)

		return
	}

	details := map[string]any{"ledger": ledgerName}

	logEntry := exactlyOneLog("create-transaction", logs, details)

	ledgerLog := logEntry.GetPayload().GetApply().GetLog()
	created, ok := ledgerLog.GetData().GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction)
	if !ok {
		unexpectedLogPayload("create-transaction", logEntry, details)
	}

	writeCreated(w, created.CreatedTransaction)
}
