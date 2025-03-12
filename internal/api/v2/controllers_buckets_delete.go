package v2

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/controller/system"
)

func deleteBucket(systemController system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucketName := r.URL.Query().Get("name")
		if bucketName == "" {
			api.BadRequest(w, "MISSING_PARAMETER", errors.New("missing bucket name parameter"))
			return
		}

		err := systemController.MarkBucketAsDeleted(r.Context(), bucketName)
		if err != nil {
			api.BadRequest(w, "BUCKET_DELETION_ERROR", err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}
