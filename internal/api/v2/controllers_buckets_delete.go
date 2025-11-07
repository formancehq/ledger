package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
)

// deleteBucket constructs an HTTP handler that deletes the bucket specified by the "bucket" URL parameter.
// The handler invokes systemController.DeleteBucket with the request context; if deletion fails it responds with an internal server error, otherwise it responds with 204 No Content.
func deleteBucket(systemController system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket := chi.URLParam(r, "bucket")

		err := systemController.DeleteBucket(r.Context(), bucket)
		if err != nil {
			common.InternalServerError(w, r, err)
			return
		}

		api.NoContent(w)
	}
}
