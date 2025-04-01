package api

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/controller/system"
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
		func(next http.Handler) http.Handler {
			fn := func(w http.ResponseWriter, r *http.Request) {
				defer func() {
					if rvr := recover(); rvr != nil {
						if rvr == http.ErrAbortHandler {
							// we don't recover http.ErrAbortHandler so the response
							// to the client is aborted, this should not be logged
							panic(rvr)
						}

						if debug {
							middleware.PrintPrettyStack(rvr)
						}

						otlp.RecordError(r.Context(), fmt.Errorf("%s", rvr))

						w.WriteHeader(http.StatusInternalServerError)
					}
				}()

				next.ServeHTTP(w, r)
			}

			return http.HandlerFunc(fn)
		},
	)

	commonMiddlewares := []func(http.Handler) http.Handler{
		middleware.RequestLogger(api.NewLogFormatter()),
	}

	v2Router := v2.NewRouter(
		systemController,
		authenticator,
		version,
		debug,
		v2.WithTracer(routerOptions.tracer),
		v2.WithMiddlewares(commonMiddlewares...),
		v2.WithBulkerFactory(routerOptions.bulkerFactory),
		v2.WithBulkHandlerFactories(map[string]bulking.HandlerFactory{
			"application/json": bulking.NewJSONBulkHandlerFactory(routerOptions.bulkMaxSize),
			"application/vnd.formance.ledger.api.v2.bulk+script-stream": bulking.NewScriptStreamBulkHandlerFactory(),
		}),
		v2.WithPaginationConfig(routerOptions.paginationConfig),
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
	tracer           trace.Tracer
	bulkMaxSize      int
	bulkerFactory    bulking.BulkerFactory
	paginationConfig common.PaginationConfig
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

func WithBulkerFactory(bf bulking.BulkerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkerFactory = bf
	}
}

func WithPaginationConfiguration(paginationConfig common.PaginationConfig) RouterOption {
	return func(ro *routerOptions) {
		ro.paginationConfig = paginationConfig
	}
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithBulkMaxSize(DefaultBulkMaxSize),
	WithPaginationConfiguration(common.PaginationConfig{
		MaxPageSize:     bunpaginate.MaxPageSize,
		DefaultPageSize: bunpaginate.QueryDefaultPageSize,
	}),
}

const DefaultBulkMaxSize = 100
