package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type setDefaultEnforcementModeRequest struct {
	EnforcementMode string `json:"enforcementMode"`
}

// handleSetDefaultEnforcementMode handles PUT /{ledgerName}/account-types/default-enforcement-mode.
func (s *Server) handleSetDefaultEnforcementMode(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	var body setDefaultEnforcementModeRequest
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	if body.EnforcementMode == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("enforcementMode is required"))

		return
	}

	mode, err := parseEnforcementMode(body.EnforcementMode)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	_, err = s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_SetDefaultEnforcementMode{
			SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
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
