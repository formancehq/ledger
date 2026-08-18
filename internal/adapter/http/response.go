package http

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
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

// writeOKChecked writes a 200 OK response like writeOK, but marshals the body
// to a buffer BEFORE writing any header, so a marshal failure is routed through
// handleError (a clean 500 with no partial body) instead of being appended to
// an already-committed 200 stream.
//
// This matters for the audit surface: audit DTOs render chain-bound submessages
// (callerSnapshot, idempotency, signature) via protojson, whose MarshalJSON can
// genuinely fail (e.g. invalid UTF-8) and MUST propagate as an error rather than
// a valid-looking truncated record (invariant #7).
//
// The transactions list, chapters and single-log routes are the second
// legitimate caller (EN-1622): their payload types carry a hand-written
// MarshalJSON that marshals a metadata map and can genuinely fail, so the
// streaming writeOK would append an error object to an already-committed 200.
// The remaining list/get handlers keep writeOK: their struct marshaling cannot
// fail, so buffering would only add an allocation.
func writeOKChecked(w http.ResponseWriter, r *http.Request, data any) {
	body, err := json.Marshal(BaseResponse[any]{Data: data})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// writeCheckedBody marshals an already-shaped response body to a buffer BEFORE
// writing any header, so a marshal failure becomes a clean 500 through
// handleError instead of an error object appended to an already-committed
// status. Callers that want the standard BaseResponse envelope should use
// writeCheckedStatus instead.
//
// This is the ConfigStd (json.MarshalWrite) side of the package's two encoders;
// writeOKChecked is the ConfigDefault side. A route must keep the encoder it
// already had, so the choice between them follows the writer the route used
// before, never preference (EN-1779).
func writeCheckedBody(w http.ResponseWriter, r *http.Request, statusCode int, body any) {
	var buf bytes.Buffer

	if err := json.MarshalWrite(&buf, body); err != nil {
		handleError(w, r, err)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(buf.Bytes())
}

// writeCheckedStatus writes a JSON response like writeJSONResponse, but marshals
// the body to a buffer BEFORE writing any header, so a marshal failure is routed
// through handleError (a clean 500 with no partial body) instead of being
// appended to an already-committed status.
//
// It exists alongside writeOKChecked because the two use DIFFERENT encoders:
// writeOKChecked marshals with json.Marshal (sonic ConfigDefault) while this
// goes through writeCheckedBody's json.MarshalWrite (sonic ConfigStd, which
// HTML-escapes and appends a trailing newline). A route must keep the encoder it
// already had, so the choice between them is determined by the writer the route
// used before, never by preference (EN-1779).
//
// It differs from writeCheckedBody only in wrapping data in the BaseResponse
// envelope: routes that build their own top-level envelope (bulk,
// prepared-query) must call writeCheckedBody so they are not double-wrapped.
func writeCheckedStatus(w http.ResponseWriter, r *http.Request, statusCode int, data any) {
	writeCheckedBody(w, r, statusCode, BaseResponse[any]{Data: data})
}

// writeProtoOK writes a 200 OK response whose `data` is a single protobuf
// message serialized via protojson, which renders protobuf-JSON camelCase from
// the descriptor (e.g. lastIndexedSequence) rather than the snake_case Go
// `json:` tags protoc-gen emits.
//
// Use this ONLY for messages that have no custom MarshalJSON. protojson works
// off protobuf reflection and ignores json.Marshaler, so for a message that has
// one it silently discards the intended shape — that was EN-1622, where the
// transactions list leaked amount:{v0} and timestamp:{data} while the detail
// route emitted decimals and RFC3339. When the message has a MarshalJSON, that
// method IS the public contract: use writeOKChecked.
//
// encoder_contract_test.go enforces this split.
func writeProtoOK(w http.ResponseWriter, msg proto.Message) {
	raw, err := protojson.Marshal(msg)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", err)

		return
	}

	writeOK(w, json.RawValue(raw))
}

// writeProtoListOK writes a 200 OK response whose `data` is a JSON array of
// protobuf messages, each serialized via protojson (see writeProtoOK for when
// that is correct — messages with no custom MarshalJSON only). protojson has no
// slice entry point, so each element is marshaled individually and assembled
// into the array here; sonic needs no equivalent, since it walks a slice and
// calls MarshalJSON per element. A nil/empty slice serializes as `[]`, matching
// the drained-cursor list handlers.
func writeProtoListOK[T proto.Message](w http.ResponseWriter, msgs []T) {
	var buf bytes.Buffer

	buf.WriteByte('[')

	for i, msg := range msgs {
		if i > 0 {
			buf.WriteByte(',')
		}

		raw, err := protojson.Marshal(msg)
		if err != nil {
			writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", err)

			return
		}

		buf.Write(raw)
	}

	buf.WriteByte(']')

	writeOK(w, json.RawValue(buf.Bytes()))
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
