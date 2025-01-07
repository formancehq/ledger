package api

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/pkg/errors"
)

func (a *API) deleteConnector(w http.ResponseWriter, r *http.Request) {
	if err := a.backend.DeleteConnector(r.Context(), a.connectorID(r)); err != nil {
		switch {
		case errors.Is(err, ErrConnectorNotFound("")):
			api.NotFound(w, err)
		case errors.Is(err, ErrConnectorUsed("")):
			api.BadRequest(w, "VALIDATION", err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
