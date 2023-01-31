package server

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/logging"
	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/formancehq/webhooks/pkg/storage"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func (h *serverHandler) testOneConfigHandle(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, PathParamId)
	cfgs, err := h.store.FindManyConfigs(r.Context(), map[string]any{"id": id})
	if err == nil {
		if len(cfgs) == 0 {
			logging.GetLogger(r.Context()).Errorf("GET %s/%s%s: %s", PathConfigs, id, PathTest, storage.ErrConfigNotFound)
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		logging.GetLogger(r.Context()).Infof("GET %s/%s%s", PathConfigs, id, PathTest)
		attempt, err := webhooks.MakeAttempt(r.Context(), h.httpClient, nil, uuid.NewString(),
			uuid.NewString(), 0, cfgs[0], []byte(`{"data":"test"}`), true)
		if err != nil {
			logging.GetLogger(r.Context()).Errorf("GET %s/%s%s: %s", PathConfigs, id, PathTest, err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		} else {
			logging.GetLogger(r.Context()).Infof("GET %s/%s%s", PathConfigs, id, PathTest)
			resp := api.BaseResponse[webhooks.Attempt]{
				Data: &attempt,
			}
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				logging.GetLogger(r.Context()).Errorf("json.Encoder.Encode: %s", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	} else {
		logging.GetLogger(r.Context()).Errorf("GET %s/%s%s: %s", PathConfigs, id, PathTest, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}
