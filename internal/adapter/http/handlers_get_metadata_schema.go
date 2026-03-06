package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// metadataFieldStatusJSON is the camelCase JSON DTO for MetadataFieldStatus.
type metadataFieldStatusJSON struct {
	DeclaredType  string `json:"declaredType"`
	Status        string `json:"status"`
	TotalKeys     uint64 `json:"totalKeys"`
	ConvertedKeys uint64 `json:"convertedKeys"`
}

// metadataSchemaStatusJSON is the camelCase JSON DTO for GetMetadataSchemaStatusResponse.
type metadataSchemaStatusJSON struct {
	AccountFields     map[string]*metadataFieldStatusJSON `json:"accountFields"`
	TransactionFields map[string]*metadataFieldStatusJSON `json:"transactionFields"`
}

func toFieldStatusJSON(fs *servicepb.MetadataFieldStatus) *metadataFieldStatusJSON {
	return &metadataFieldStatusJSON{
		DeclaredType:  commonpb.MetadataTypeToString(fs.GetDeclaredType()),
		Status:        commonpb.ConversionStatusToString(fs.GetStatus()),
		TotalKeys:     fs.GetTotalKeys(),
		ConvertedKeys: fs.GetConvertedKeys(),
	}
}

func toSchemaStatusJSON(resp *servicepb.GetMetadataSchemaStatusResponse) *metadataSchemaStatusJSON {
	result := &metadataSchemaStatusJSON{
		AccountFields:     make(map[string]*metadataFieldStatusJSON, len(resp.GetAccountFields())),
		TransactionFields: make(map[string]*metadataFieldStatusJSON, len(resp.GetTransactionFields())),
	}
	for k, v := range resp.GetAccountFields() {
		result.AccountFields[k] = toFieldStatusJSON(v)
	}

	for k, v := range resp.GetTransactionFields() {
		result.TransactionFields[k] = toFieldStatusJSON(v)
	}

	return result
}

// handleGetMetadataSchema handles GET /{ledgerName}/metadata-schema to get schema status.
func (s *Server) handleGetMetadataSchema(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))

		return
	}

	resp, err := s.backend.GetMetadataSchemaStatus(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, toSchemaStatusJSON(resp))
}
