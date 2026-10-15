package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// revertTransactionRequestBody is the JSON body of the revert endpoint.
type revertTransactionRequestBody struct {
	Force           bool           `json:"force"`
	AtEffectiveDate bool           `json:"atEffectiveDate"`
	Metadata        map[string]any `json:"metadata"`
}

// handleRevertTransaction handles POST /{ledgerName}/transactions/{transactionId}/revert to revert a transaction.
func (s *Server) handleRevertTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	transactionID, ok := requireTransactionID(w, r)
	if !ok {
		return
	}

	// Read the whole body through the router's MaxBytesReader before decoding.
	// A stream decoder can stop at the first value and miss trailing data or
	// an oversized suffix. Only a zero-byte body means default revert options.
	// The typed body rejects an option of the wrong JSON type (for example
	// "atEffectiveDate":"true" or "metadata":[...]) with 400 instead of
	// silently applying the default. JSON null keeps the default.
	var reqBody revertTransactionRequestBody
	if r.Body != nil {
		body, err := io.ReadAll(r.Body)
		if err == nil && len(body) != 0 {
			if !json.Valid(body) {
				err = errors.New("expected a single valid JSON value")
			} else {
				decoder := json.NewDecoder(bytes.NewReader(body))
				decoder.UseNumber()
				err = decoder.Decode(&reqBody)
			}
		}
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

			return
		}
	}

	// Build request payload
	payload := &servicepb.RevertTransactionPayload{
		TransactionId:   transactionID,
		Force:           reqBody.Force,
		AtEffectiveDate: reqBody.AtEffectiveDate,
	}

	if reqBody.Metadata != nil {
		// Decode metadata through the shared typed-metadata path so numeric,
		// boolean and other non-string values survive losslessly (parity with
		// create-transaction / set-metadata). Invalid values (objects, arrays,
		// non-integer floats) are rejected with 400 INVALID_REQUEST instead of
		// being silently dropped (EN-1509).
		ms, err := commonpb.MetadataFromAnyMap(reqBody.Metadata)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid metadata: %w", err))

			return
		}

		payload.Metadata = ms
	}

	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_RevertTransaction{
						RevertTransaction: payload,
					},
				},
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"error":          err,
		}).Errorf("Failed to revert transaction")
		handleError(w, r, err)

		return
	}

	// Return the full reverted transaction response (includes post-commit volumes when requested)
	details := map[string]any{
		"ledger":         ledgerName,
		"transaction_id": transactionID,
	}

	logEntry := exactlyOneLog("revert-transaction", logs, details)

	ledgerLog := logEntry.GetPayload().GetApply().GetLog()
	rt, ok := ledgerLog.GetData().GetPayload().(*commonpb.LedgerLogPayload_RevertedTransaction)
	if !ok {
		panic(unexpectedLogPayload("revert-transaction", logEntry, details))
	}

	if rt.RevertedTransaction == nil {
		panic(emptyLogPayload("revert-transaction", logEntry, details))
	}

	writeMonetaryCreated(w, r, rt.RevertedTransaction)
}
