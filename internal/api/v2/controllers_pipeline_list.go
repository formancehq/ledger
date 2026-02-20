package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v4/api"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

func listPipelines(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelines, err := systemController.ListPipelines(r.Context())
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.RenderCursor(w, *pipelines)
	}

}
