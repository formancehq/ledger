package middlewares

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/contextlogger"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry"
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
