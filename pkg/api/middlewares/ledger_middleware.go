package middlewares

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/opentelemetry/tracer"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
)

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomTraceID(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return string(b)
}

func LedgerMiddleware(
	resolver controllers.Backend,
) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := chi.URLParam(r, "ledger")
			if name == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			ctx, span := tracer.Start(r.Context(), name)
			defer span.End()

			r = r.WithContext(ctx)

			loggerFields := map[string]any{
				"ledger": name,
			}
			if span.SpanContext().TraceID().IsValid() {
				loggerFields["trace-id"] = span.SpanContext().TraceID().String()
			} else {
				loggerFields["trace-id"] = randomTraceID(10)
			}
			r = r.WithContext(logging.ContextWithFields(r.Context(), loggerFields))

			l, err := resolver.GetLedger(r.Context(), name)
			if err != nil {
				apierrors.ResponseError(w, r, err)
				return
			}
			// TODO(polo/gfyrag): close ledger if not used for x minutes
			// defer l.Close(context.Background())
			// When close, we have to decrease the active ledgers counter:
			// globalMetricsRegistry.ActiveLedgers.Add(r.Context(), -1)

			r = r.WithContext(controllers.ContextWithLedger(r.Context(), l))

			handler.ServeHTTP(w, r)
		})
	}
}
