package v2

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func listBuckets(systemController system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buckets, err := systemController.ListBucketsWithStatus(r.Context())
		if err != nil {
			switch {
			case errors.Is(err, storagecommon.ErrInvalidQuery{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.RenderJSON(w, buckets)
	}
}
