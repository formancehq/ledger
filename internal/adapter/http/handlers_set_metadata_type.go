package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleSetMetadataType handles PUT /{ledgerName}/metadata-schema/{targetType}/{key}.
func (s *Server) handleSetMetadataType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	targetTypeStr := chi.URLParam(r, "targetType")

	targetType, err := commonpb.ParseTargetType(targetTypeStr)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	key := chi.URLParam(r, "key")
	if key == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("metadata key is required"))

		return
	}

	var body struct {
		Type string `json:"type"`
	}
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	mdType, err := commonpb.ParseMetadataType(body.Type)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	_, err = s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
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
