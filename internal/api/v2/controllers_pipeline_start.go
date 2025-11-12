package v2

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/api"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

func startPipeline(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := systemController.StartPipeline(r.Context(), getPipelineID(r)); err != nil {
			switch {
			case errors.Is(err, ledger.ErrPipelineNotFound("")):
				api.NotFound(w, err)
			case errors.Is(err, ledger.ErrAlreadyStarted("")) ||
				errors.Is(err, ledgercontroller.ErrInUsePipeline("")):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}

}
