package v2

import (
	ledger "github.com/formancehq/ledger/internal"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/api"
)

func readPipeline(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipeline, err := systemController.GetPipeline(r.Context(), getPipelineID(r))
		if err != nil {
			switch {
			case errors.Is(err, ledger.ErrPipelineNotFound("")):
				api.NotFound(w, err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.Ok(w, pipeline)
	}

}
