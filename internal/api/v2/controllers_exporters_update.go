package v2

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

func updateExporter(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		exporterID := chi.URLParam(r, "exporterID")
		common.WithBody[ledger.ExporterConfiguration](w, r, func(req ledger.ExporterConfiguration) {
			err := systemController.UpdateExporter(r.Context(), exporterID, req)
			if err != nil {
				switch {
				case errors.Is(err, systemcontroller.ErrInvalidDriverConfiguration{}):
					api.BadRequest(w, "VALIDATION", err)
				case errors.Is(err, systemcontroller.ErrExporterNotFound("")):
					api.NotFound(w, err)
				default:
					api.InternalServerError(w, r, err)
				}
				return
			}

			w.WriteHeader(http.StatusNoContent)
		})
	}
}
