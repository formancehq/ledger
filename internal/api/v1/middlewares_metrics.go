package v1

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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

func MetricsMiddleware(globalMetricsRegistry metrics.GlobalRegistry) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attrs := []attribute.KeyValue{}

			ctx := r.Context()
			name := chi.URLParam(r, "ledger")
			if name != "" {
				attrs = append(attrs, attribute.String("ledger", name))
			}

			recorder := newStatusRecorder(w)

			start := time.Now()
			h.ServeHTTP(recorder, r)
			latency := time.Since(start)

			attrs = append(attrs,
				attribute.String("route", chi.RouteContext(r.Context()).RoutePattern()))

			globalMetricsRegistry.APILatencies().Record(ctx, latency.Milliseconds(), metric.WithAttributes(attrs...))

			attrs = append(attrs, attribute.Int("status", recorder.Status))
			globalMetricsRegistry.StatusCodes().Add(ctx, 1, metric.WithAttributes(attrs...))
		})
	}
}
