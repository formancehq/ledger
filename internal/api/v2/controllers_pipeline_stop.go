package v2

import (
	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/pkg/errors"
	"net/http"
)

func stopPipeline(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := systemController.StopPipeline(r.Context(), getPipelineID(r)); err != nil {
			switch {
			case errors.Is(err, ledger.ErrPipelineNotFound("")):
				api.NotFound(w, err)
			case errors.Is(err, ledgercontroller.ErrInUsePipeline("")):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}
