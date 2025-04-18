package v2

import (
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/api"
)

type PipelineConfiguration struct {
	ConnectorID string `json:"connectorID"`
}

func createPipeline(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[PipelineConfiguration](w, r, func(req PipelineConfiguration) {
			p, err := systemController.CreatePipeline(r.Context(), ledger.PipelineConfiguration{
				ConnectorID: req.ConnectorID,
				Ledger:      common.LedgerFromContext(r.Context()).Info().Name,
			})
			if err != nil {
				switch {
				case errors.Is(err, systemcontroller.ErrConnectorNotFound("")) ||
					errors.Is(err, ledger.ErrPipelineAlreadyExists{}) ||
					errors.Is(err, ledgercontroller.ErrInUsePipeline("")):
					api.BadRequest(w, "VALIDATION", err)
				default:
					api.InternalServerError(w, r, err)
				}
				return
			}

			api.Created(w, p)
		})
	}
}
