package middlewares

import (
	"bytes"
	"context"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/opentelemetry"
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

func Transaction(locker Locker) func(c *gin.Context) {
	return func(c *gin.Context) {

		ctx, span := opentelemetry.Start(c.Request.Context(), "Ledger locking")
		defer span.End()

		c.Request = c.Request.WithContext(ctx)

		unlock, err := locker.Lock(c.Request.Context(), c.Param("ledger"))
		if err != nil {
			panic(err)
		}
		defer unlock(context.Background()) // Use a background context instead of the request one as it could have been cancelled

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
				apierrors.ResponseError(c, err)
				return
			}
		}

		if err := bufferedWriter.WriteResponse(); err != nil {
			_ = c.Error(err)
		}
	}
}
