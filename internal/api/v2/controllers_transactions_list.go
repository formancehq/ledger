package v2

import (
	"net/http"

	"errors"
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
		q := ledgercontroller.NewListTransactionsQuery(*options)

		if r.URL.Query().Get("order") == "effective" {
			q.Column = "timestamp"
		}
		if r.URL.Query().Get("reverse") == "true" {
			q.Order = bunpaginate.OrderAsc
		}

		return pointer.For(q), nil
	})
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.ListTransactions(r.Context(), *query)
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)
}
