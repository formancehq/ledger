package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleDeleteNumscript handles DELETE /{ledgerName}/numscripts/{name} to delete a numscript.
func (s *Server) handleDeleteNumscript(w http.ResponseWriter, r *http.Request) {
	ledger := chi.URLParam(r, "ledgerName")
	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))

		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_DeleteNumscript{
			DeleteNumscript: &servicepb.DeleteNumscriptRequest{
				Ledger: ledger,
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
