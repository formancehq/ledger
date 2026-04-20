package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"

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

		order := paginate.Order(paginate.OrderDesc)
		if api.QueryParamBool(r, "reverse") {
			order = paginate.OrderAsc
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

		api.RenderCursor(w, *paginate.MapCursor(cursor, func(tx ledger.Transaction) any {
			return renderTransaction(r, tx)
		}))
	}
}
