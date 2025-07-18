package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v3/api"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"
)

func deleteExporter(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := systemController.DeleteExporter(r.Context(), getExporterID(r)); err != nil {
			switch {
			case errors.Is(err, systemcontroller.ErrExporterNotFound("")):
				api.NotFound(w, err)
			case errors.Is(err, systemcontroller.ErrExporterUsed("")):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
