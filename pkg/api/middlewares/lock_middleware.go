package middlewares

import (
	"context"
	"net/http"

	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/opentelemetry"
	"github.com/go-chi/chi/v5"
)

func Lock(locker ledger.Locker) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, span := opentelemetry.Start(r.Context(), "Wait ledger lock")
			defer span.End()

			r = r.WithContext(ctx)

			unlock, err := locker.Lock(r.Context(), chi.URLParam(r, "ledger"))
			if err != nil {
				panic(err)
			}
			defer unlock(context.Background()) // Use a background context instead of the request one as it could have been cancelled

			handler.ServeHTTP(w, r)
		})
	}

}
