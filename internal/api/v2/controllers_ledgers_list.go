package v2

import (
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/controller/system"
)

func listLedgers(b system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		query, err := bunpaginate.Extract[ledgercontroller.ListLedgersQuery](r, func() (*ledgercontroller.ListLedgersQuery, error) {
			pageSize, err := bunpaginate.GetPageSize(r)
			if err != nil {
				return nil, err
			}

			return pointer.For(ledgercontroller.NewListLedgersQuery(pageSize)), nil
		})
		if err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		ledgers, err := b.ListLedgers(r.Context(), *query)
		if err != nil {
			switch {
			case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
				api.BadRequest(w, ErrValidation, err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.RenderCursor(w, *ledgers)
	}
}
