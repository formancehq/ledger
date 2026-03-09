package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

type addAccountTypeRequest struct {
	Name            string `json:"name"`
	Pattern         string `json:"pattern"`
	EnforcementMode string `json:"enforcementMode,omitempty"`
}

// handleAddAccountType handles POST /{ledgerName}/account-types
func (s *Server) handleAddAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	var body addAccountTypeRequest
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	if body.Name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("name is required"))
		return
	}

	if body.Pattern == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("pattern is required"))
		return
	}

	mode := commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
	if body.EnforcementMode != "" {
		var err error
		mode, err = parseEnforcementMode(body.EnforcementMode)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)
			return
		}
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger: ledgerName,
				AccountType: &commonpb.AccountType{
					Name:            body.Name,
					Pattern:         body.Pattern,
					EnforcementMode: mode,
				},
			},
		},
	})
	if err != nil {
		handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
