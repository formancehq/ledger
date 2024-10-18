package api

import (
	"context"
	"net/http"
	"time"

	"errors"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/health"
	"github.com/formancehq/ledger/internal/api/backend"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
)

func NewRouter(
	backend backend.Backend,
	healthController *health.HealthController,
	globalMetricsRegistry metrics.GlobalRegistry,
	a auth.Authenticator,
	readOnly bool,
	debug bool,
) chi.Router {
	mux := chi.NewRouter()
	mux.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			handler.ServeHTTP(w, r)
		})
	})
	mux.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 55*time.Second)
			defer func() {
				cancel()
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					w.WriteHeader(http.StatusRequestTimeout)
				}
			}()

			r = r.WithContext(ctx)

			handler.ServeHTTP(w, r)
		})
	})
	if readOnly {
		mux.Use(ReadOnly)
	}
	v2Router := v2.NewRouter(backend, healthController, globalMetricsRegistry, a, debug)
	mux.Handle("/v2*", http.StripPrefix("/v2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chi.RouteContext(r.Context()).Reset()
		v2Router.ServeHTTP(w, r)
	})))
	mux.Handle("/*", v1.NewRouter(backend, healthController, globalMetricsRegistry, a, debug))

	return mux
}
