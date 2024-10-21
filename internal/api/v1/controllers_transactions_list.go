package v1

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func listTransactions(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	query, err := bunpaginate.Extract[ledgercontroller.ListTransactionsQuery](r, func() (*ledgercontroller.ListTransactionsQuery, error) {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			return nil, err
		}
		options.QueryBuilder = buildGetTransactionsQuery(r)

		return pointer.For(ledgercontroller.NewListTransactionsQuery(*options)), nil
	})
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.ListTransactions(r.Context(), *query)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.RenderCursor(w, *bunpaginate.MapCursor(cursor, mapTransactionToV1))
}
