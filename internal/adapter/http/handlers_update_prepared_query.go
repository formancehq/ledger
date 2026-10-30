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
		Target *string         `json:"target"`
		Filter json.RawMessage `json:"filter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	filterInput := bytes.TrimSpace(body.Filter)
	if len(filterInput) == 0 {
		if body.Target != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("filter is required when updating target; use null to clear it"))

			return
		}
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

	// New clients supply the target so public id is decoded for the intended
	// entity before the order enters Raft. Targetless legacy requests retain
	// their stored target and are validated there by the FSM.
	target := commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	var explicitTarget *commonpb.QueryTarget
	if body.Target != nil {
		var err error
		target, err = parsePreparedQueryTarget(*body.Target)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
		explicitTarget = target.Enum()
	}
	filter, err := filterexpr.DecodeDualFormatStructuralOnly(body.Filter, target)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
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
				Target: explicitTarget,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
