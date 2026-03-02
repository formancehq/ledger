package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// handleUpdatePreparedQuery handles PUT /{ledgerName}/prepared-queries/{name}
func (s *Server) handleUpdatePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	queryName := chi.URLParam(r, "queryName")
	if ledgerName == "" || queryName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name and query name are required"))
		return
	}

	var body struct {
		Filter *commonpb.QueryFilter `json:"filter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
				Ledger: ledgerName,
				Name:   queryName,
				Filter: body.Filter,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
