package common

import (
	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/platform/postgres"
	"net/http"
)

func HandleCommonErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, postgres.ErrTooManyClient{}):
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
	default:
		api.InternalServerError(w, r, err)
	}
}
