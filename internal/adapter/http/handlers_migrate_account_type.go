package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

type migrateAccountTypeRequest struct {
	TargetPattern string `json:"targetPattern"`
}

// handleMigrateAccountType handles POST /{ledgerName}/account-types/{typeName}/migrate.
func (s *Server) handleMigrateAccountType(w http.ResponseWriter, r *http.Request) {
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

	var body migrateAccountTypeRequest
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	if body.TargetPattern == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("targetPattern is required"))

		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_MigrateAccountType{
			MigrateAccountType: &servicepb.MigrateAccountTypeLedgerRequest{
				Ledger:        ledgerName,
				Name:          typeName,
				TargetPattern: body.TargetPattern,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusAccepted)
}
