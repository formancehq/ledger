package api

import (
	ingester "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/pkg/errors"
)

func (a *API) createConnector(w http.ResponseWriter, r *http.Request) {
	common.WithBody[ingester.ConnectorConfiguration](w, r, func(req ingester.ConnectorConfiguration) {
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
