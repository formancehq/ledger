package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/go-chi/chi/v5"
)

// handleGetAccountType handles GET /{ledgerName}/account-types/{typeName}
func (s *Server) handleGetAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
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

	at, exists := ledgerInfo.AccountTypes[typeName]
	if !exists {
		handleError(w, r, &domain.BusinessError{
			Err: &domain.ErrAccountTypeNotFound{Name: typeName},
		})
		return
	}

	writeOK(w, toAccountTypeJSON(at))
}
