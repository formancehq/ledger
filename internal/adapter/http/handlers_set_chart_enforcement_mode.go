package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// handleSetChartEnforcementMode handles PUT /{ledgerName}/chart-of-accounts/enforcement-mode
func (s *Server) handleSetChartEnforcementMode(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	var body struct {
		Mode string `json:"mode"`
	}
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	mode, err := parseEnforcementMode(body.Mode)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	_, err = s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_SetChartEnforcementMode{
			SetChartEnforcementMode: &servicepb.SetChartEnforcementModeLedgerRequest{
				Ledger:          ledgerName,
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
