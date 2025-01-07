package api

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/api"
)

func (a *API) readPipeline(w http.ResponseWriter, r *http.Request) {
	pipeline, err := a.backend.GetPipeline(r.Context(), a.pipelineID(r))
	if err != nil {
		switch {
		case errors.Is(err, ErrPipelineNotFound("")):
			api.NotFound(w, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.Ok(w, pipeline)
}
