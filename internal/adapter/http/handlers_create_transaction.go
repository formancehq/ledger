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
	logs, err := s.applyUnsigned(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
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
		writeInternalServerError(w, r, errors.New("no log returned from apply"))

		return
	}

	ledgerLog := logs[0].GetPayload().GetApply().GetLog()
	ct, ok := ledgerLog.GetData().GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction)
	if !ok {
		writeInternalServerError(w, r, errors.New("unexpected log payload type"))

		return
	}
	writeCreated(w, ct.CreatedTransaction)
}
