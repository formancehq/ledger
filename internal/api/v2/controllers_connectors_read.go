package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v3/api"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"
)

func getConnector(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		connector, err := systemController.GetConnector(r.Context(), getConnectorID(r))
		if err != nil {
			switch {
			case errors.Is(err, systemcontroller.ErrConnectorNotFound("")):
				api.NotFound(w, err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.Ok(w, connector)
	}
}
