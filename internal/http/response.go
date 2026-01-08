package http

import (
	"encoding/json/v2"
	"net/http"
)

// ErrorResponse represents an error response structure
type ErrorResponse struct {
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// BaseResponse represents a successful response structure with data wrapper
type BaseResponse[T any] struct {
	Data T `json:"data"`
}

// WriteJSONResponse writes a JSON response with the given status code and data
func WriteJSONResponse(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.MarshalWrite(w, data); err != nil {
		// If encoding fails, we can't write a proper error response
		// The connection might be broken, so we just log and return
		return
	}
}

// WriteErrorResponse writes an error response with the given status code, error code, and error
func WriteErrorResponse(w http.ResponseWriter, statusCode int, errorCode string, err error) {
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}
	WriteJSONResponse(w, statusCode, ErrorResponse{
		ErrorCode:    errorCode,
		ErrorMessage: errorMsg,
	})
}

// writeJSONResponse writes a JSON response with the given status code and data (internal helper)
func writeJSONResponse(w http.ResponseWriter, statusCode int, data any) {
	WriteJSONResponse(w, statusCode, data)
}

// writeErrorResponse writes an error response with the given status code, error code, and error (internal helper)
func writeErrorResponse(w http.ResponseWriter, statusCode int, errorCode string, err error) {
	WriteErrorResponse(w, statusCode, errorCode, err)
}

// writeOK writes a 200 OK response with the given data wrapped in BaseResponse
func writeOK(w http.ResponseWriter, data any) {
	writeJSONResponse(w, http.StatusOK, BaseResponse[any]{
		Data: data,
	})
}

// writeCreated writes a 201 Created response with the given data wrapped in BaseResponse
func writeCreated(w http.ResponseWriter, data any) {
	writeJSONResponse(w, http.StatusCreated, BaseResponse[any]{
		Data: data,
	})
}

// writeBadRequest writes a 400 Bad Request response
func writeBadRequest(w http.ResponseWriter, errorCode string, err error) {
	writeErrorResponse(w, http.StatusBadRequest, errorCode, err)
}

// writeInternalServerError writes a 500 Internal Server Error response
func writeInternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", err)
}

// queryParamBool returns true if the query parameter exists and is "true"
func queryParamBool(r *http.Request, key string) bool {
	return r.URL.Query().Get(key) == "true"
}
