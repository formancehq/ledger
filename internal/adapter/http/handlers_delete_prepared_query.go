package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleDeletePreparedQuery handles DELETE /{ledgerName}/prepared-queries/{name}.
func (s *Server) handleDeletePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	queryName, ok := requirePathParameter(w, r, "queryName", "query name")
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), "", &servicepb.Request{
		Type: &servicepb.Request_DeletePreparedQuery{
			DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
				Ledger: ledgerName,
				Name:   queryName,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
