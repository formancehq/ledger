package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/controller/system"
	"net/http"
)

func deleteConnector(systemController system.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := systemController.DeleteConnector(r.Context(), getConnectorID(r)); err != nil {
			switch {
			case errors.Is(err, system.ErrConnectorNotFound("")):
				api.NotFound(w, err)
			case errors.Is(err, system.ErrConnectorUsed("")):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
