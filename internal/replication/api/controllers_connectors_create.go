package api

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	ingester "github.com/formancehq/ledger/internal/replication"
	"github.com/pkg/errors"
)

func (a *API) createConnector(w http.ResponseWriter, r *http.Request) {
	withBody[ingester.ConnectorConfiguration](w, r, func(req ingester.ConnectorConfiguration) {
		connector, err := a.backend.CreateConnector(r.Context(), req)
		if err != nil {
			switch {
			case errors.Is(err, ErrInvalidConnectorConfiguration{}):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.Created(w, connector)
	})
}
