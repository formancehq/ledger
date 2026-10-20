package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// handleGetAccountType handles GET /{ledgerName}/account-types/{typeName}.
func (s *Server) handleGetAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	typeName, ok := requirePathParameter(w, r, "typeName", "type name")
	if !ok {
		return
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	at, exists := ledgerInfo.GetAccountTypes()[typeName]
	if !exists {
		handleError(w, r, &domain.BusinessError{
			Err: &domain.ErrAccountTypeNotFound{Name: typeName},
		})

		return
	}

	writeOK(w, toAccountTypeJSON(at))
}
