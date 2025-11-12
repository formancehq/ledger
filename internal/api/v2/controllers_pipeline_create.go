package v2

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/api"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

type PipelineConfiguration struct {
	ExporterID string `json:"exporterID"`
}

func createPipeline(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[PipelineConfiguration](w, r, func(req PipelineConfiguration) {
			p, err := systemController.CreatePipeline(r.Context(), ledger.PipelineConfiguration{
				ExporterID: req.ExporterID,
				Ledger:     common.LedgerFromContext(r.Context()).Info().Name,
			})
			if err != nil {
				switch {
				case errors.Is(err, systemcontroller.ErrExporterNotFound("")) ||
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
