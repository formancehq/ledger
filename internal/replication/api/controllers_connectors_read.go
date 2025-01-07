package api

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/pkg/errors"
)

func (a *API) getConnector(w http.ResponseWriter, r *http.Request) {
	connector, err := a.backend.GetConnector(r.Context(), a.connectorID(r))
	if err != nil {
		switch {
		case errors.Is(err, ErrConnectorNotFound("")):
			api.NotFound(w, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.Ok(w, connector)
}
