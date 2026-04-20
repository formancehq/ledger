package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func listSchemas(paginationConfig storagecommon.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		// Handle sort and order parameters
		column := "created_at"
		if sort := r.URL.Query().Get("sort"); sort != "" {
			column = sort
		}

		order := paginate.Order(paginate.OrderDesc)
		if orderParam := r.URL.Query().Get("order"); orderParam != "" {
			switch orderParam {
			case "asc":
				order = paginate.Order(paginate.OrderAsc)
			case "desc":
				order = paginate.Order(paginate.OrderDesc)
			}
		}

		query, err := getPaginatedQuery[any](r, paginationConfig, column, order)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.ListSchemas(r.Context(), query)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *paginate.MapCursor(cursor, func(schema ledger.Schema) any {
			return renderSchema(r, schema)
		}))
	}
}
