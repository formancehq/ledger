package api

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/api"
	ingester "github.com/formancehq/ledger/internal/replication"
)

func (a *API) createPipeline(w http.ResponseWriter, r *http.Request) {
	withBody[ingester.PipelineConfiguration](w, r, func(req ingester.PipelineConfiguration) {
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
