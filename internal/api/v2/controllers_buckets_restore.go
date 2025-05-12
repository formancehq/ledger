package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
)

func restoreBucket(systemController system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket := chi.URLParam(r, "bucket")
		
		err := systemController.RestoreBucket(r.Context(), bucket)
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}
		
		api.NoContent(w)
	}
}
