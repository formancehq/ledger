package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
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
	if body.Target == nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("target is required"))

		return
	}

	filterInput := bytes.TrimSpace(body.Filter)
	if len(filterInput) == 0 {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("filter is required when updating target; use null to clear it"))

		return
	}

	target, err := parsePreparedQueryTarget(*body.Target)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
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
				Target: target.Enum(),
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
