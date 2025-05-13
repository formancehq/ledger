package v2

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func listBuckets(systemController system.Controller, paginationConfig bunpaginate.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := common.GetColumnPaginatedQuery[any](r, paginationConfig)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		buckets, err := systemController.ListBucketsWithStatus(r.Context(), query)
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
