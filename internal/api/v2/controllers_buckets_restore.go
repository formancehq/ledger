package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
)

// restoreBucket returns an HTTP handler that restores the bucket identified by the URL parameter "bucket".
// It invokes the provided system.Controller's RestoreBucket with the request context and the extracted bucket name.
// On success it responds with HTTP 204 No Content; on failure it writes an internal server error response.
func restoreBucket(systemController system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket := chi.URLParam(r, "bucket")

		err := systemController.RestoreBucket(r.Context(), bucket)
		if err != nil {
			common.InternalServerError(w, r, err)
			return
		}

		api.NoContent(w)
	}
}
