package server

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/logging"
	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/formancehq/webhooks/pkg/storage"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func (h *serverHandler) activateOneConfigHandle(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, PathParamId)
	c, err := h.store.UpdateOneConfigActivation(r.Context(), id, true)
	if err == nil {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s", PathConfigs, id, PathActivate)
		resp := api.BaseResponse[webhooks.Config]{
			Data: &c,
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			logging.GetLogger(r.Context()).Errorf("json.Encoder.Encode: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	} else if errors.Is(err, storage.ErrConfigNotFound) {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s: %s", PathConfigs, id, PathActivate, storage.ErrConfigNotFound)
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	} else if errors.Is(err, storage.ErrConfigNotModified) {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s: %s", PathConfigs, id, PathActivate, storage.ErrConfigNotModified)
		w.WriteHeader(http.StatusNotModified)
	} else {
		logging.GetLogger(r.Context()).Errorf("PUT %s/%s%s: %s", PathConfigs, id, PathActivate, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func (h *serverHandler) deactivateOneConfigHandle(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, PathParamId)
	c, err := h.store.UpdateOneConfigActivation(r.Context(), id, false)
	if err == nil {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s", PathConfigs, id, PathDeactivate)
		resp := api.BaseResponse[webhooks.Config]{
			Data: &c,
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			logging.GetLogger(r.Context()).Errorf("json.Encoder.Encode: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	} else if errors.Is(err, storage.ErrConfigNotFound) {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s: %s", PathConfigs, id, PathDeactivate, storage.ErrConfigNotFound)
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	} else if errors.Is(err, storage.ErrConfigNotModified) {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s: %s", PathConfigs, id, PathDeactivate, storage.ErrConfigNotModified)
		w.WriteHeader(http.StatusNotModified)
	} else {
		logging.GetLogger(r.Context()).Errorf("PUT %s/%s%s: %s", PathConfigs, id, PathDeactivate, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}
