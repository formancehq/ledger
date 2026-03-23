package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
)

// handleGetAccountType handles GET /{ledgerName}/account-types/{typeName}.
func (s *Server) handleGetAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	typeName := chi.URLParam(r, "typeName")
	if typeName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("type name is required"))

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
