package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v2/api"
	ingester "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
	"net/http"
)

func createConnector(systemController system.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[ingester.ConnectorConfiguration](w, r, func(req ingester.ConnectorConfiguration) {
			connector, err := systemController.CreateConnector(r.Context(), req)
			if err != nil {
				switch {
				case errors.Is(err, system.ErrInvalidConnectorConfiguration{}):
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
