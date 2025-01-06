package v1

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/common"
)

func listTransactions(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	paginatedQuery, err := getColumnPaginatedQuery[any](r, "id", bunpaginate.OrderDesc)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}
	paginatedQuery.Options.Builder = buildGetTransactionsQuery(r)
	paginatedQuery.Options.Expand = []string{"volumes"}

	cursor, err := l.ListTransactions(r.Context(), *paginatedQuery)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.RenderCursor(w, *bunpaginate.MapCursor(cursor, mapTransactionToV1))
}
