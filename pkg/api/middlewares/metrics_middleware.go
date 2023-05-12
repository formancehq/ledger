package middlewares

import (
	"net/http"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/attribute"
)

type statusRecorder struct {
	http.ResponseWriter
	Status int
}

func newStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{ResponseWriter: w}
}

func (r *statusRecorder) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}

func MetricsMiddleware(globalMetricsRegistry metrics.GlobalMetricsRegistry) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attrs := []attribute.KeyValue{}

			ctx := r.Context()
			name := chi.URLParam(r, "ledger")
			if name != "" {
				attrs = append(attrs, attribute.String("ledger", name))
			}

			recorder := newStatusRecorder(w)

			start := core.Now()
			h.ServeHTTP(recorder, r)
			latency := time.Since(start.Time)

			attrs = append(attrs,
				attribute.String("route", chi.RouteContext(r.Context()).RoutePattern()))

			globalMetricsRegistry.APILatencies().Record(ctx, latency.Milliseconds(), attrs...)

			attrs = append(attrs, attribute.Int("status", recorder.Status))
			globalMetricsRegistry.StatusCodes().Add(ctx, 1, attrs...)
		})
	}
}
