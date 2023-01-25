package worker

import (
	"net/http"

	"github.com/formancehq/go-libs/logging"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
)

const (
	PathHealthCheck = "/_healthcheck"
)

func newWorkerHandler() http.Handler {
	h := chi.NewRouter()
	h.Use(otelchi.Middleware("webhooks"))
	h.Get(PathHealthCheck, healthCheckHandle)

	return h
}

func healthCheckHandle(_ http.ResponseWriter, r *http.Request) {
	logging.GetLogger(r.Context()).Infof("health check OK")
}
