package idempotency

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

type responseWriter struct {
	gin.ResponseWriter
	buf    *bytes.Buffer
	writer io.Writer
}

func (r *responseWriter) Write(i []byte) (int, error) {
	return r.writer.Write(i)
}

func (r *responseWriter) Bytes() []byte {
	return r.buf.Bytes()
}

var _ http.ResponseWriter = &responseWriter{}

func newResponseWriter(underlying gin.ResponseWriter) *responseWriter {
	buf := bytes.NewBuffer(make([]byte, 0))
	return &responseWriter{
		ResponseWriter: underlying,
		buf:            buf,
		writer:         io.MultiWriter(underlying, buf),
	}
}
