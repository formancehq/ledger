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

func (h *serverHandler) changeSecretHandle(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, PathParamId)
	sec := webhooks.Secret{}
	if err := decodeJSONBody(r, &sec, true); err != nil {
		var errIB *errInvalidBody
		if errors.As(err, &errIB) {
			http.Error(w, errIB.Error(), errIB.status)
		} else {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
		logging.GetLogger(r.Context()).Errorf("decodeJSONBody: %s", err)
		return
	}

	if err := sec.Validate(); err != nil {
		logging.GetLogger(r.Context()).Errorf("invalid secret: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	c, err := h.store.UpdateOneConfigSecret(r.Context(), id, sec.Secret)
	if err == nil {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s", PathConfigs, id, PathChangeSecret)
		resp := api.BaseResponse[webhooks.Config]{
			Data: &c,
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			logging.GetLogger(r.Context()).Errorf("json.Encoder.Encode: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	} else if errors.Is(err, storage.ErrConfigNotFound) {
		logging.GetLogger(r.Context()).Infof("PUT %s/%s%s: %s", PathConfigs, id, PathChangeSecret, storage.ErrConfigNotFound)
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	} else {
		logging.GetLogger(r.Context()).Errorf("PUT %s/%s%s: %s", PathConfigs, id, PathChangeSecret, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}
