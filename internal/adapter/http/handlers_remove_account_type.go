package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleRemoveAccountType handles DELETE /{ledgerName}/account-types/{typeName}.
func (s *Server) handleRemoveAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	typeName := chi.URLParam(r, "typeName")
	if typeName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("type name is required"))

		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_RemoveAccountType{
			RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
				Ledger: ledgerName,
				Name:   typeName,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
