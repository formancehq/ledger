package http

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/types/known/structpb"
)

// handleSaveAccountMetadata handles POST /{ledgerName}/accounts/{address}/metadata to save account metadata
func (s *Server) handleSaveAccountMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	address := chi.URLParam(r, "address")
	if address == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("account address is required"))
		return
	}

	// Decode request body into metadata
	var inputMetadataStruct *structpb.Struct
	if err := json.UnmarshalRead(r.Body, &inputMetadataStruct); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	var inputMetadata map[string]string
	if inputMetadataStruct != nil {
		inputMetadata = ledgerpb.StructToMetadata(inputMetadataStruct)
	}

	// Build service.Parameters[*ledgerpb.SaveAccountMetadataRequest]
	params := service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
		DryRun:         r.URL.Query().Get("dryRun") == "true",
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input: &ledgerpb.SaveAccountMetadataRequestPayload{
			Address:  address,
			Metadata: inputMetadata,
		},
	}

	ledgerCluster, err := s.cluster.GetLedgerCluster(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Call ledger service
	_, err = ledgerCluster.SaveAccountMetadata(r.Context(), ledgerName, params)
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "address": address, "error": err}).Errorf("Failed to save account metadata")
		handleError(w, r, err)
		return
	}

	// Return 204 No Content (no Content-Type header for 204)
	w.WriteHeader(http.StatusNoContent)
}
