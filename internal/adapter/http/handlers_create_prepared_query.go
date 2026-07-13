package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleCreatePreparedQuery handles POST /{ledgerName}/prepared-queries.
func (s *Server) handleCreatePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	var body struct {
		Name   string          `json:"name"`
		Target string          `json:"target"`
		Filter json.RawMessage `json:"filter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	if body.Name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("name is required"))

		return
	}

	target, err := parsePreparedQueryTarget(body.Target)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	filter, err := decodePreparedQueryFilter(body.Filter)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	// Reject conditions invalid on this query's specific target early (precise
	// 400). Admission + FSM re-validate with the same domain helper, so gRPC
	// callers and the update path are covered too (EN-1504).
	if verr := domain.ValidateFilterForTarget(filter, target); verr != nil {
		writeBadRequest(w, "INVALID_REQUEST", verr)

		return
	}

	_, err = s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_CreatePreparedQuery{
			CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
				Ledger: ledgerName,
				Query: &commonpb.PreparedQuery{
					Name:   body.Name,
					Filter: filter,
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
