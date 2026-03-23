package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleCreatePreparedQuery handles POST /{ledgerName}/prepared-queries.
func (s *Server) handleCreatePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	var body struct {
		Name   string                `json:"name"`
		Target string                `json:"target"`
		Filter *commonpb.QueryFilter `json:"filter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	if body.Name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("name is required"))

		return
	}

	target := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	if body.Target == "TRANSACTIONS" {
		target = commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_CreatePreparedQuery{
			CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   body.Name,
					Ledger: ledgerName,
					Filter: body.Filter,
					Target: target,
				},
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
