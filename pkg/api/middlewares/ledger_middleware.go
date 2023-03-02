package middlewares

import (
	"context"
	"net/http"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/contextlogger"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/opentelemetry"
	"github.com/go-chi/chi/v5"
)

type LedgerMiddleware struct {
	resolver *ledger.Resolver
}

func NewLedgerMiddleware(resolver *ledger.Resolver) LedgerMiddleware {
	return LedgerMiddleware{
		resolver: resolver,
	}
}

func (m *LedgerMiddleware) LedgerMiddleware() func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := chi.URLParam(r, "ledger")
			if name == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			ctx, span := opentelemetry.Start(r.Context(), name)
			defer span.End()

			r = r.WithContext(ctx)
			r = contextlogger.WrapRequest(r)

			l, err := m.resolver.GetLedger(r.Context(), name)
			if err != nil {
				apierrors.ResponseError(w, r, err)
				return
			}
			defer l.Close(context.Background())

			r = r.WithContext(controllers.ContextWithLedger(r.Context(), l))

			handler.ServeHTTP(w, r)
		})
	}
}
