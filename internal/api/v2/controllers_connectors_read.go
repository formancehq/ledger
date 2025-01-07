package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/controller/system"
	"net/http"
)

func getConnector(systemController system.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		connector, err := systemController.GetConnector(r.Context(), getConnectorID(r))
		if err != nil {
			switch {
			case errors.Is(err, system.ErrConnectorNotFound("")):
				api.NotFound(w, err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.Ok(w, connector)
	}
}
