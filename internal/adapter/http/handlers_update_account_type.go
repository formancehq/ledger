package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

type updateAccountTypeRequest struct {
	EnforcementMode string `json:"enforcementMode"`
}

// handleUpdateAccountType handles PATCH /{ledgerName}/account-types/{typeName}.
func (s *Server) handleUpdateAccountType(w http.ResponseWriter, r *http.Request) {
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

	var body updateAccountTypeRequest
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	mode, err := parseEnforcementMode(body.EnforcementMode)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	_, err = s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_UpdateAccountType{
			UpdateAccountType: &servicepb.UpdateAccountTypeLedgerRequest{
				Ledger:          ledgerName,
				Name:            typeName,
				EnforcementMode: mode,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
