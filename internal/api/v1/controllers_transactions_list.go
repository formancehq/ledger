package v1

import (
	"errors"
	"github.com/formancehq/go-libs/platform/postgres"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/pointer"
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
		options.QueryBuilder, err = buildGetTransactionsQuery(r)
		if err != nil {
			return nil, err
		}
		return pointer.For(ledgercontroller.NewListTransactionsQuery(*options)), nil
	})
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.ListTransactions(r.Context(), *query)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrTooManyClient{}):
			api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *bunpaginate.MapCursor(cursor, mapExpandedTransactionToV1))
}
