package server

import (
	"net/http"

	"github.com/formancehq/webhooks/pkg/storage"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
)

const (
	PathHealthCheck  = "/_healthcheck"
	PathConfigs      = "/configs"
	PathTest         = "/test"
	PathActivate     = "/activate"
	PathDeactivate   = "/deactivate"
	PathChangeSecret = "/secret/change"
	PathId           = "/{" + PathParamId + "}"
	PathParamId      = "id"
)

type serverHandler struct {
	*chi.Mux

	store      storage.Store
	httpClient *http.Client
}

func newServerHandler(store storage.Store, httpClient *http.Client) http.Handler {
	h := &serverHandler{
		Mux:        chi.NewRouter(),
		store:      store,
		httpClient: httpClient,
	}

	h.Mux.Use(otelchi.Middleware("webhooks"))
	h.Mux.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			handler.ServeHTTP(w, r)
		})
	})
	h.Mux.Get(PathHealthCheck, h.HealthCheckHandle)
	h.Mux.Get(PathConfigs, h.getManyConfigsHandle)
	h.Mux.Post(PathConfigs, h.insertOneConfigHandle)
	h.Mux.Delete(PathConfigs+PathId, h.deleteOneConfigHandle)
	h.Mux.Get(PathConfigs+PathId+PathTest, h.testOneConfigHandle)
	h.Mux.Put(PathConfigs+PathId+PathActivate, h.activateOneConfigHandle)
	h.Mux.Put(PathConfigs+PathId+PathDeactivate, h.deactivateOneConfigHandle)
	h.Mux.Put(PathConfigs+PathId+PathChangeSecret, h.changeSecretHandle)

	return h
}
