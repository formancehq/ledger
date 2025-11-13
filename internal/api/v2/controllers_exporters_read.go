package v2

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

func getExporter(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		exporter, err := systemController.GetExporter(r.Context(), getExporterID(r))
		if err != nil {
			switch {
			case errors.Is(err, systemcontroller.ErrExporterNotFound("")):
				api.NotFound(w, err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.Ok(w, exporter)
	}
}
