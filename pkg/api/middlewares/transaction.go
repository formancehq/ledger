package middlewares

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
)

type bufferedResponseWriter struct {
	http.ResponseWriter
	buf        io.ReadWriter
	statusCode int
}

func (r *bufferedResponseWriter) Write(data []byte) (int, error) {
	return r.buf.Write(data)
}

func (r *bufferedResponseWriter) WriteHeader(statusCode int) {
	r.statusCode = statusCode
}

func (r *bufferedResponseWriter) WriteResponse() error {
	r.ResponseWriter.WriteHeader(r.statusCode)
	_, err := io.Copy(r.ResponseWriter, r.buf)
	return err
}

func newBufferedWriter(rw http.ResponseWriter) *bufferedResponseWriter {
	buf := bytes.NewBuffer(make([]byte, 0))
	return &bufferedResponseWriter{
		ResponseWriter: rw,
		buf:            buf,
		statusCode:     200,
	}
}

func Transaction(locker Locker) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, span := opentelemetry.Start(r.Context(), "Wait ledger lock")
			defer span.End()

			r = r.WithContext(ctx)

			bufferedWriter := newBufferedWriter(w)

			func() {
				unlock, err := locker.Lock(r.Context(), chi.URLParam(r, "ledger"))
				if err != nil {
					panic(err)
				}
				defer unlock(context.Background()) // Use a background context instead of the request one as it could have been cancelled

				ctx, span = opentelemetry.Start(r.Context(), "Ledger locked")
				defer span.End()
				r = r.WithContext(ctx)
				r = r.WithContext(storage.TransactionalContext(r.Context()))
				defer func() {
					_ = storage.RollbackTransaction(r.Context())
				}()

				handler.ServeHTTP(bufferedWriter, r)

				if bufferedWriter.statusCode >= 200 && bufferedWriter.statusCode < 300 &&
					storage.IsTransactionRegistered(r.Context()) {
					if err := storage.CommitTransaction(r.Context()); err != nil {
						apierrors.ResponseError(w, r, err)
						return
					}
				}
			}()

			if err := bufferedWriter.WriteResponse(); err != nil {
				logging.FromContext(r.Context()).Errorf(err.Error())
			}
		})
	}

}
