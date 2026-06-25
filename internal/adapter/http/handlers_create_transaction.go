package http

import (
	"errors"
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
	// expandVolumes, …) — the default protoc-gen-go tags are snake_case and
	// would silently drop multi-word keys (#452).
	req := &servicepb.CreateTransactionPayload{}

	err := json.UnmarshalRead(r.Body, req)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	// Call ledger service via Apply
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

	// Return the service response directly - JSON encoding will handle it
	if len(logs) == 0 {
		unreachable("create-transaction apply returned no log", map[string]any{"ledger": ledgerName})
	}

	ledgerLog := logs[0].GetPayload().GetApply().GetLog()
	switch payload := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		writeCreated(w, payload.CreatedTransaction)
	case *commonpb.LedgerLogPayload_OrderSkipped:
		// `skippable_reasons` opted in for a reason that fired: no state
		// mutation happened. Reply 200 (not 201) and surface the skip
		// reason + context so clients can correlate without an extra GET.
		writeOK(w, OrderSkippedResponse{
			Skipped: true,
			Reason:  payload.OrderSkipped.GetReason().String(),
			Context: payload.OrderSkipped.GetContext(),
		})
	default:
		writeInternalServerError(w, r, errors.New("unexpected log payload type"))
	}
}

// OrderSkippedResponse is the HTTP body returned when an Apply request opted
// into `skippableReasons` and the FSM matched one of the listed reasons. The
// `skipped: true` flag is the canonical branch point for clients
// distinguishing this 200 response from the 201 Created path.
type OrderSkippedResponse struct {
	Skipped bool              `json:"skipped"`
	Reason  string            `json:"reason"`
	Context map[string]string `json:"context,omitempty"`
}
