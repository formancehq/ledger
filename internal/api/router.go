package api

import (
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/controller/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/ledger/internal/api/common"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/go-chi/chi/v5"
)

// todo: refine textual errors
func NewRouter(
	systemController system.Controller,
	authenticator auth.Authenticator,
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
		cors.New(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowCredentials: true,
		}).Handler,
		common.LogID(),
	)

	commonMiddlewares := []func(http.Handler) http.Handler{
		middleware.RequestLogger(api.NewLogFormatter()),
	}

	if debug {
		commonMiddlewares = append(commonMiddlewares, func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				trace.SpanFromContext(r.Context()).
					SetAttributes(attribute.String("raw-query", r.URL.RawQuery))

				handler.ServeHTTP(w, r)
			})
		})
	}

	v2Router := v2.NewRouter(
		systemController,
		authenticator,
		debug,
		v2.WithTracer(routerOptions.tracer),
		v2.WithMiddlewares(commonMiddlewares...),
		v2.WithBulkMaxSize(routerOptions.bulkMaxSize),
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
		v1.WithMiddlewares(commonMiddlewares...),
	))

	return mux
}

type routerOptions struct {
	tracer      trace.Tracer
	bulkMaxSize int
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithBulkMaxSize(bulkMaxSize int) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkMaxSize = bulkMaxSize
	}
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithBulkMaxSize(DefaultBulkMaxSize),
}

const DefaultBulkMaxSize = 100
