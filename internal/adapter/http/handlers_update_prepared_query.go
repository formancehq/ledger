package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleUpdatePreparedQuery handles PUT /{ledgerName}/prepared-queries/{name}.
func (s *Server) handleUpdatePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	queryName, ok := requirePathParameter(w, r, "queryName", "query name")
	if !ok {
		return
	}

	var body struct {
		Filter json.RawMessage `json:"filter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	filterInput := bytes.TrimSpace(body.Filter)
	if len(filterInput) == 0 {
		queries, err := s.backend.ListPreparedQueries(r.Context(), ledgerName)
		if err != nil {
			handleError(w, r, err)

			return
		}

		for _, query := range queries {
			if query.GetName() == queryName {
				w.WriteHeader(http.StatusNoContent)

				return
			}
		}

		handleError(w, r, &domain.ErrPreparedQueryNotFound{Ledger: ledgerName, Name: queryName})

		return
	}

	// The update request carries only the new filter, not the target: the target
	// is immutable and lives on the stored prepared query. Per-target filter
	// validity is enforced against the stored target by the FSM (processUpdatePreparedQuery →
	// domain.ValidateFilterForTarget), which is authoritative on apply. An update
	// cannot smuggle in a condition invalid for the query's target (EN-1504).
	// This handler stays purely structural: it accepts either the structured v2
	// JSON DSL or a JSON-quoted textual filterexpr expression (EN-1511) and defers
	// the target gate to the FSM. Bare-field resolution uses a non-audit target
	// (EN-1549). For id, the handler reads the stored target below and re-decodes
	// LOGS filters so the public field maps to LogIdCondition.
	filter, err := filterexpr.DecodeDualFormatStructuralOnly(body.Filter, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	if filterexpr.HasTransactionIDCondition(filter) {
		// The update payload has no target. Resolve the public id field against
		// the immutable target of the stored query before submitting it.
		queries, listErr := s.backend.ListPreparedQueries(r.Context(), ledgerName)
		if listErr != nil {
			handleError(w, r, listErr)

			return
		}
		found := false
		for _, query := range queries {
			if query.GetName() != queryName {
				continue
			}
			found = true
			if query.GetTarget() == commonpb.QueryTarget_QUERY_TARGET_LOGS {
				filter, err = filterexpr.DecodeDualFormatStructuralOnly(body.Filter, query.GetTarget())
				if err != nil {
					writeBadRequest(w, "INVALID_REQUEST", err)

					return
				}
			}

			break
		}
		if !found {
			handleError(w, r, &domain.ErrPreparedQueryNotFound{Ledger: ledgerName, Name: queryName})

			return
		}
	}

	if filter == nil && !bytes.Equal(filterInput, []byte("null")) {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("filter must contain at least one condition"))

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
