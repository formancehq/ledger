package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// handleMigrateAccountType handles POST /{ledgerName}/account-types/{typeName}/migrate
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

	type migrateRequest struct {
		TargetType string `json:"targetType"`
		DryRun     bool   `json:"dryRun"`
	}
	var req migrateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}
	if req.TargetType == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("targetType is required"))
		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_MigrateAccountType{
			MigrateAccountType: &servicepb.MigrateAccountTypeLedgerRequest{
				Ledger:     ledgerName,
				SourceType: typeName,
				TargetType: req.TargetType,
				DryRun:     req.DryRun,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
