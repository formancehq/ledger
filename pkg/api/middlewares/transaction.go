package middlewares

import (
	"bytes"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/errors"
	"github.com/numary/ledger/pkg/storage"
)

type bufferedResponseWriter struct {
	gin.ResponseWriter
	buf        io.ReadWriter
	statusCode int
}

func (r *bufferedResponseWriter) WriteString(s string) (int, error) {
	return r.Write([]byte(s))
}

func (r *bufferedResponseWriter) WriteHeaderNow() {}

func (r *bufferedResponseWriter) Write(data []byte) (int, error) {
	return r.buf.Write(data)
}

func (r *bufferedResponseWriter) WriteHeader(statusCode int) {
	r.statusCode = statusCode
}

func (r *bufferedResponseWriter) Status() int {
	return r.statusCode
}

func (r *bufferedResponseWriter) WriteResponse() error {
	r.ResponseWriter.WriteHeader(r.statusCode)
	_, err := io.Copy(r.ResponseWriter, r.buf)
	return err
}

func newBufferedWriter(rw gin.ResponseWriter) *bufferedResponseWriter {
	buf := bytes.NewBuffer(make([]byte, 0))
	return &bufferedResponseWriter{
		ResponseWriter: rw,
		buf:            buf,
	}
}

func Transaction() func(c *gin.Context) {
	return func(c *gin.Context) {
		bufferedWriter := newBufferedWriter(c.Writer)
		c.Request = c.Request.WithContext(storage.TransactionalContext(c.Request.Context()))
		c.Writer = bufferedWriter
		defer func() {
			_ = storage.RollbackTransaction(c.Request.Context())
		}()

		c.Next()

		if c.Writer.Status() >= 200 && c.Writer.Status() < 300 &&
			storage.IsTransactionRegistered(c.Request.Context()) {
			if err := storage.CommitTransaction(c.Request.Context()); err != nil {
				errors.ResponseError(c, err)
				return
			}
		}

		if err := bufferedWriter.WriteResponse(); err != nil {
			_ = c.Error(err)
		}
	}
}
