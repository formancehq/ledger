package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/protohelpers"
)

// handleRemoveMetadataType handles DELETE /{ledgerName}/metadata-schema/{targetType}/{key}.
func (s *Server) handleRemoveMetadataType(w http.ResponseWriter, r *http.Request) {
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

	_, err = s.applyUnsigned(r.Context(), "", &ledgerpb.Request{
		Type: &ledgerpb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &ledgerpb.RemoveMetadataFieldTypeRequest{
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
