package http

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/protohelpers"
)

// handleSetMetadataType handles PUT /{ledgerName}/metadata-schema/{targetType}/{key}.
func (s *Server) handleSetMetadataType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	targetTypeStr := chi.URLParam(r, "targetType")

	targetType, err := protohelpers.ParseTargetType(targetTypeStr)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	key, ok := requireMetadataKey(w, r)
	if !ok {
		return
	}

	var body struct {
		Type string `json:"type"`
	}
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	mdType, err := protohelpers.ParseMetadataType(body.Type)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	_, err = s.applyUnsigned(r.Context(), "", &commonpb.Request{
		Type: &commonpb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &commonpb.SetMetadataFieldTypeRequest{
				Ledger:     ledgerName,
				TargetType: targetType,
				Key:        key,
				Type:       mdType,
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":     ledgerName,
			"targetType": targetTypeStr,
			"key":        key,
			"type":       body.Type,
			"error":      err,
		}).Errorf("Failed to set metadata field type")
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
