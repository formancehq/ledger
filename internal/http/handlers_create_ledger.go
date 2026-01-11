package http

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/systempb"
	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/encoding/protojson"
)

// handleCreateLedger handles POST /{ledgerName} to create a new ledger
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	if r.Body == nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("request body is required"))
		return
	}

	req := &systempb.CreateLedgerRequest{}
	if err := json.UnmarshalRead(r.Body, &req, json.WithUnmarshalers(json.UnmarshalFunc(protojson.Unmarshal))); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}
	req.Name = ledgerName
	data, _ := json.Marshal(req)
	fmt.Println(string(data))

	// Create ledger via cluster
	ledgerInfo, err := s.backend.CreateLedger(r.Context(), req)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return the ledger info wrapped in BaseResponse
	writeCreated(w, ledgerInfo)
}
