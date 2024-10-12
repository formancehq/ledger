package api

import (
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/logging"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/ledger/internal/api/common"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/go-chi/chi/v5"
)

// todo: refine textual errors
func NewRouter(
	systemController system.Controller,
	authenticator auth.Authenticator,
	logger logging.Logger,
	version string,
	debug bool,
	opts ...RouterOption,
) chi.Router {

	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	mux := chi.NewRouter()
	mux.Use(
		middleware.Recoverer,
		func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))

				handler.ServeHTTP(w, r)
			})
		},
		cors.New(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowCredentials: true,
		}).Handler,
		common.LogID(),
	)

	v2Router := v2.NewRouter(
		systemController,
		authenticator,
		debug,
		v2.WithTracer(routerOptions.tracer),
	)
	mux.Handle("/v2*", http.StripPrefix("/v2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chi.RouteContext(r.Context()).Reset()
		v2Router.ServeHTTP(w, r)
	})))
	mux.Handle("/*", v1.NewRouter(
		systemController,
		authenticator,
		version,
		debug,
		v1.WithTracer(routerOptions.tracer),
	))

	return mux
}

type routerOptions struct {
	tracer trace.Tracer
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
}
