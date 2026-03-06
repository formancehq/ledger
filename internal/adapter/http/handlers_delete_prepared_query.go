package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleDeletePreparedQuery handles DELETE /{ledgerName}/prepared-queries/{name}.
func (s *Server) handleDeletePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")

	queryName := chi.URLParam(r, "queryName")
	if ledgerName == "" || queryName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name and query name are required"))

		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
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
