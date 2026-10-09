package http

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// HeaderBigIntAsString is the v2-compatible per-request monetary encoding opt-in.
const HeaderBigIntAsString = "Formance-Bigint-As-String"

func monetaryEncoding(r *http.Request) jsonv2.Options {
	// Match v2 exactly, including its lack of whitespace trimming.
	switch strings.ToLower(r.Header.Get(HeaderBigIntAsString)) {
	case "true", "yes", "y", "1":
		return commonpb.MonetaryAmountsAsStrings()
	default:
		return commonpb.MonetaryAmountsAsNumbers()
	}
}

// writeMonetaryJSONResponse is the scoped option-aware writer for responses
// containing postings, volumes or balances. Other responses retain Sonic.
func writeMonetaryJSONResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.MarshalWriteWithOptions(w, data, monetaryEncoding(r)); err != nil {
		// Preserve the existing streaming failure contract: headers are committed.
		_, _ = fmt.Fprintf(w, `{"errorCode":"INTERNAL_ERROR","errorMessage":"failed to marshal response: %s"}`, err.Error())
	}
}

func writeMonetaryOK(w http.ResponseWriter, r *http.Request, data any) {
	writeMonetaryJSONResponse(w, r, http.StatusOK, BaseResponse[any]{Data: data})
}

func writeMonetaryCreated(w http.ResponseWriter, r *http.Request, data any) {
	writeMonetaryJSONResponse(w, r, http.StatusCreated, BaseResponse[any]{Data: data})
}

func writeMonetaryOKChecked(w http.ResponseWriter, r *http.Request, data any) {
	writeOKCheckedEncoded(w, r, data, func(value any) ([]byte, error) {
		return json.MarshalWithOptions(value, monetaryEncoding(r))
	})
}

func writeMonetaryPageOK(w http.ResponseWriter, r *http.Request, data any, links pageLinks) {
	writeCheckedJSONResponse(w, r, pagedResponse{Data: data, Next: links.Next, Previous: links.Previous, HasMore: links.HasMore}, func(value any) ([]byte, error) {
		return json.MarshalWithOptions(value, monetaryEncoding(r))
	})
}
