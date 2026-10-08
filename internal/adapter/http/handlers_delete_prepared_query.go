package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// handleDeletePreparedQuery handles DELETE /{ledgerName}/prepared-queries/{name}.
func (s *Server) handleDeletePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	queryName := chi.URLParam(r, "queryName")
	if queryName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("query name is required"))

		return
	}

	_, err := s.applyUnsigned(r.Context(), "", &ledgerpb.Request{
		Type: &ledgerpb.Request_DeletePreparedQuery{
			DeletePreparedQuery: &ledgerpb.DeletePreparedQueryRequest{
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
