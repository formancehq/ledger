package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// handleCreateLedger handles POST /{ledgerName} to create a new ledger
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse optional metadata from request body
	var metadata map[string]string
	if r.Body != nil && r.ContentLength > 0 {
		var body struct {
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.UnmarshalRead(r.Body, &body); err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
			return
		}
		metadata = body.Metadata
	}

	// Create ledger via Apply
	log, err := s.backend.Apply(r.Context(), &servicepb.Action{
		Type: &servicepb.Action_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name:     ledgerName,
				Metadata: metadata,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return the ledger info wrapped in BaseResponse
	writeCreated(w, log.GetCreateLedger().GetInfo())
}
