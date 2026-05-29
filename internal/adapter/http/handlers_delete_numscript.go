package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleDeleteNumscript handles DELETE /{ledgerName}/numscripts/{name} to delete a numscript.
func (s *Server) handleDeleteNumscript(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))

		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_DeleteNumscript{
			DeleteNumscript: &servicepb.DeleteNumscriptRequest{
				Ledger: ledgerName,
				Name:   name,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
