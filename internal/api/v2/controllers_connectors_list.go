package v2

import (
	"github.com/formancehq/go-libs/v3/api"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"
)

func listConnectors(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		connectors, err := systemController.ListConnectors(r.Context())
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.RenderCursor(w, *connectors)
	}
}
