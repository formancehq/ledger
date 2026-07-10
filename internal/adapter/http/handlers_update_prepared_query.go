package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleUpdatePreparedQuery handles PUT /{ledgerName}/prepared-queries/{name}.
func (s *Server) handleUpdatePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	queryName := chi.URLParam(r, "queryName")
	if queryName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("query name is required"))

		return
	}

	var body struct {
		Filter json.RawMessage `json:"filter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	// The update request carries only the new filter, not the target: the target
	// is immutable and lives on the stored prepared query, which this handler
	// does not read. Per-target filter validity is therefore enforced against the
	// stored target by the FSM (processUpdatePreparedQuery →
	// domain.ValidateFilterForTarget), the only layer that sees it — so an update
	// cannot smuggle in a condition invalid for the query's target (EN-1504).
	// This handler stays purely structural.
	filter, err := decodePreparedQueryFilter(body.Filter)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	_, err = s.applyUnsigned(r.Context(), "", &servicepb.Request{
		Type: &servicepb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
				Ledger: ledgerName,
				Name:   queryName,
				Filter: filter,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
