package api

import (
	ingester "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/api"
)

func (a *API) createPipeline(w http.ResponseWriter, r *http.Request) {
	common.WithBody[ingester.PipelineConfiguration](w, r, func(req ingester.PipelineConfiguration) {
		p, err := a.backend.CreatePipeline(r.Context(), req)
		if err != nil {
			switch {
			case errors.Is(err, ErrConnectorNotFound("")) ||
				errors.Is(err, ErrPipelineAlreadyExists{}) ||
				errors.Is(err, ErrInUsePipeline("")):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.Created(w, p)
	})
}
