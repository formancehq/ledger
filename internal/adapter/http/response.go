package http

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/query"
)

// ErrorResponse represents an error response structure.
type ErrorResponse struct {
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// BaseResponse represents a successful response structure with data wrapper.
type BaseResponse[T any] struct {
	Data T `json:"data"`
}

// writeJSONResponse writes a JSON response with the given status code and data.
// It streams JSON directly into the ResponseWriter to avoid allocating an
// intermediate byte buffer.
func writeJSONResponse(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.MarshalWrite(w, data); err != nil {
		// Headers already sent; log but can't change the status code.
		_, _ = fmt.Fprintf(w, `{"errorCode":"INTERNAL_ERROR","errorMessage":"failed to marshal response: %s"}`, err.Error())
	}
}

// writeErrorResponse writes an error response with the given status code, error code, and error.
func writeErrorResponse(w http.ResponseWriter, statusCode int, errorCode string, err error) {
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}

	writeJSONResponse(w, statusCode, ErrorResponse{
		ErrorCode:    errorCode,
		ErrorMessage: errorMsg,
	})
}

// writeOK writes a 200 OK response with the given data wrapped in BaseResponse.
func writeOK(w http.ResponseWriter, data any) {
	writeJSONResponse(w, http.StatusOK, BaseResponse[any]{
		Data: data,
	})
}

// writeCreated writes a 201 Created response with the given data wrapped in BaseResponse.
func writeCreated(w http.ResponseWriter, data any) {
	writeJSONResponse(w, http.StatusCreated, BaseResponse[any]{
		Data: data,
	})
}

// writeBadRequest writes a 400 Bad Request response.
// If the underlying error is a MaxBytesError (body too large), it writes
// 413 Request Entity Too Large instead.
func writeBadRequest(w http.ResponseWriter, errorCode string, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		writeErrorResponse(w, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", err)

		return
	}

	writeErrorResponse(w, http.StatusBadRequest, errorCode, err)
}

// writeInternalServerError writes a 500 Internal Server Error response.
//
// The raw err is logged server-side with a correlation ID; the client only
// receives a generic message carrying that same ID. This is the sanitization
// boundary for handleError's fallthrough: any non-domain error (wrapped Pebble
// errors, filesystem paths, invariant strings) must never reach the client
// (mirrors the gRPC adapter's convertToGRPCError default branch, #375).
func writeInternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	id := correlationID(r)

	logging.FromContext(r.Context()).WithFields(map[string]any{
		"correlation_id": id,
	}).Errorf("HTTP unmapped handler error: %v", err)

	writeErrorResponse(
		w,
		http.StatusInternalServerError,
		"INTERNAL_ERROR",
		fmt.Errorf("internal server error (correlation ID: %s)", id),
	)
}

// queryParamBool returns true if the query parameter exists and is "true".
func queryParamBool(r *http.Request, key string) bool {
	return r.URL.Query().Get(key) == "true"
}

const (
	httpHeaderQueryProfile       = "X-Query-Profile"
	httpHeaderQueryProfileResult = "X-Query-Profile-Result"
)

// wantsHTTPProfile returns true if the client sent the X-Query-Profile header.
func wantsHTTPProfile(r *http.Request) bool {
	return r.Header.Get(httpHeaderQueryProfile) != ""
}

// writeProfileHeader serializes the query profile as base64-encoded protobuf
// and sets it as the X-Query-Profile-Result response header.
func writeProfileHeader(w http.ResponseWriter, profile *query.QueryProfile) {
	if profile == nil {
		return
	}

	pb := profile.ToProto()

	data, err := proto.Marshal(pb)
	if err != nil {
		return
	}

	w.Header().Set(httpHeaderQueryProfileResult, base64.StdEncoding.EncodeToString(data))
}
