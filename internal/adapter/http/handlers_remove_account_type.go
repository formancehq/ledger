package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
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

	_, err := s.applyUnsigned(r.Context(), "", &ledgerpb.Request{
		Type: &ledgerpb.Request_RemoveAccountType{
			RemoveAccountType: &ledgerpb.RemoveAccountTypeLedgerRequest{
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
