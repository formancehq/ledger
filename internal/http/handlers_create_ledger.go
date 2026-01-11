package http

import (
	"errors"
	"fmt"
	"io"
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("unable to read request body: %w", err))
		return
	}

	req := &systempb.CreateLedgerRequest{}
	unmarshalOptions := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := unmarshalOptions.Unmarshal(body, req); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	if req.StoreDriver == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("store driver is required"))
		return
	}

	req.Name = ledgerName

	// Create ledger via cluster
	ledgerInfo, err := s.backend.CreateLedger(r.Context(), req)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return the ledger info wrapped in BaseResponse
	writeCreated(w, ledgerInfo)
}
