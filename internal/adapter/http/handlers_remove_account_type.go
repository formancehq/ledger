package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleRemoveAccountType handles DELETE /{ledgerName}/account-types/{typeName}.
func (s *Server) handleRemoveAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	typeName, ok := requirePathParameter(w, r, "typeName", "type name")
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), "", &servicepb.Request{
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
