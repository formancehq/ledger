package idempotency

import (
	"bytes"
	"io"
	"net/http"
)

type responseWriter struct {
	http.ResponseWriter
	buf        *bytes.Buffer
	writer     io.Writer
	statusCode int
}

func (r *responseWriter) Write(i []byte) (int, error) {
	return r.writer.Write(i)
}

func (r *responseWriter) Bytes() []byte {
	return r.buf.Bytes()
}

func (r *responseWriter) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

var _ http.ResponseWriter = &responseWriter{}

func newResponseWriter(underlying http.ResponseWriter) *responseWriter {
	buf := bytes.NewBuffer(make([]byte, 0))
	return &responseWriter{
		ResponseWriter: underlying,
		buf:            buf,
		writer:         io.MultiWriter(underlying, buf),
	}
}
