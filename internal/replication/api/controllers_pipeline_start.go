package api

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/pkg/errors"
)

func (a *API) startPipeline(w http.ResponseWriter, r *http.Request) {
	if err := a.backend.StartPipeline(r.Context(), a.pipelineID(r)); err != nil {
		switch {
		case errors.Is(err, ErrPipelineNotFound("")):
			api.NotFound(w, err)
		case errors.Is(err, ErrPipelineAlreadyStarted("")) ||
			errors.Is(err, ErrInUsePipeline("")):
			api.BadRequest(w, "VALIDATION", err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
