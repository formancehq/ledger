package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"
)

func createConnector(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[ledger.ConnectorConfiguration](w, r, func(req ledger.ConnectorConfiguration) {
			connector, err := systemController.CreateConnector(r.Context(), req)
			if err != nil {
				switch {
				case errors.Is(err, systemcontroller.ErrInvalidDriverConfiguration{}):
					api.BadRequest(w, "VALIDATION", err)
				default:
					api.InternalServerError(w, r, err)
				}
				return
			}

			api.Created(w, connector)
		})
	}
}
