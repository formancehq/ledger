package middlewares

import (
	"net/http"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/opentelemetry"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

func LedgerMiddleware(resolver controllers.Backend) func(handler http.Handler) http.Handler {
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
			r = wrapRequest(r)

			l, err := resolver.GetLedger(r.Context(), name)
			if err != nil {
				apierrors.ResponseError(w, r, err)
				return
			}
			// TODO(polo/gfyrag): close ledger if not used for x minutes
			// defer l.Close(context.Background())

			r = r.WithContext(controllers.ContextWithLedger(r.Context(), l))

			handler.ServeHTTP(w, r)
		})
	}
}

func wrapRequest(r *http.Request) *http.Request {
	span := trace.SpanFromContext(r.Context())
	contextKeyID := uuid.NewString()
	if span.SpanContext().SpanID().IsValid() {
		contextKeyID = span.SpanContext().SpanID().String()
	}
	return r.WithContext(logging.ContextWithLogger(r.Context(), logging.FromContext(r.Context()).WithFields(map[string]any{
		"contextID": contextKeyID,
	})))
}
