package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/controller/system"
)

// listLedgers constructs an HTTP handler that lists ledgers with pagination.
// The handler applies the provided pagination configuration (sorted by "id" ascending),
// reads the "includeDeleted" query parameter to include deleted ledgers when set,
// invokes the controller's ListLedgers, and renders the resulting paginated cursor.
func listLedgers(b system.Controller, paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		rq, err := getPaginatedQuery[systemstore.ListLedgersQueryPayload](
			r,
			paginationConfig,
			"id",
			bunpaginate.OrderAsc,
			func(resourceQuery *storagecommon.ResourceQuery[systemstore.ListLedgersQueryPayload]) {
				// Extract includeDeleted query parameter
				includeDeleted := api.QueryParamBool(r, "includeDeleted")
				resourceQuery.Opts.IncludeDeleted = includeDeleted
			},
		)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		ledgers, err := b.ListLedgers(r.Context(), rq)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *ledgers)
	}
}