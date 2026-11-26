package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleRemoveMetadataType handles DELETE /{ledgerName}/metadata-schema/{targetType}/{key}.
func (s *Server) handleRemoveMetadataType(w http.ResponseWriter, r *http.Request) {
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

	key, ok := requireMetadataKey(w, r)
	if !ok {
		return
	}

	_, err = s.applyUnsigned(r.Context(), "", &servicepb.Request{
		Type: &servicepb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
				Ledger:     ledgerName,
				TargetType: targetType,
				Key:        key,
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":     ledgerName,
			"targetType": targetTypeStr,
			"key":        key,
			"error":      err,
		}).Errorf("Failed to remove metadata field type")
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
