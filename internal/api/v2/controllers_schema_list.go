package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
)

func listSchemas(paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		// Handle sort and order parameters
		column := "created_at"
		if sort := r.URL.Query().Get("sort"); sort != "" {
			column = sort
		}

		order := bunpaginate.Order(bunpaginate.OrderDesc)
		if orderParam := r.URL.Query().Get("order"); orderParam != "" {
			switch orderParam {
			case "asc":
				order = bunpaginate.Order(bunpaginate.OrderAsc)
			case "desc":
				order = bunpaginate.Order(bunpaginate.OrderDesc)
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

		api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(schema ledger.Schema) any {
			return renderSchema(r, schema)
		}))
	}
}
