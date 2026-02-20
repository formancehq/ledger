package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/bun/bunpaginate"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func listTransactions(paginationConfig storagecommon.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		paginationColumn := "id"
		if r.URL.Query().Get("order") == "effective" {
			paginationColumn = "timestamp"
		}

		order := bunpaginate.Order(bunpaginate.OrderDesc)
		if api.QueryParamBool(r, "reverse") {
			order = bunpaginate.OrderAsc
		}

		rq, err := getPaginatedQuery[any](r, paginationConfig, paginationColumn, order)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.ListTransactions(r.Context(), rq)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(tx ledger.Transaction) any {
			return renderTransaction(r, tx)
		}))
	}
}
