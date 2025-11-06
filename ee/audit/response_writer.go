//go:build ee

package audit

import (
	"bytes"
	"net/http"
)

// ResponseWriterWrapper wraps http.ResponseWriter to capture response
type ResponseWriterWrapper struct {
	http.ResponseWriter
	Body       *bytes.Buffer
	StatusCode *int
}

// NewResponseWriterWrapper creates a new ResponseWriterWrapper
func NewResponseWriterWrapper(w http.ResponseWriter, buf *bytes.Buffer) *ResponseWriterWrapper {
	defaultStatus := http.StatusOK
	return &ResponseWriterWrapper{
		ResponseWriter: w,
		Body:           buf,
		StatusCode:     &defaultStatus,
	}
}

// Write writes the data to the connection and captures it
func (w *ResponseWriterWrapper) Write(b []byte) (int, error) {
	w.Body.Write(b)
	return w.ResponseWriter.Write(b)
}

// WriteHeader sends an HTTP response header with the provided status code
func (w *ResponseWriterWrapper) WriteHeader(statusCode int) {
	*w.StatusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}
