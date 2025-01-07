package api

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/api"
)

func (a *API) deletePipeline(w http.ResponseWriter, r *http.Request) {
	if err := a.backend.DeletePipeline(r.Context(), a.pipelineID(r)); err != nil {
		switch {
		case errors.Is(err, ErrPipelineNotFound("")):
			api.NotFound(w, err)
		case errors.Is(err, ErrInUsePipeline("")):
			api.BadRequest(w, "VALIDATION", err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
