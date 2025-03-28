package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/storage/resources"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/controller/system"
)

func listLedgers(b system.Controller, paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		query, err := bunpaginate.Extract[resources.ColumnPaginatedQuery[any]](r, func() (*resources.ColumnPaginatedQuery[any], error) {
			pageSize, err := bunpaginate.GetPageSize(r,
				bunpaginate.WithMaxPageSize(paginationConfig.MaxPageSize),
				bunpaginate.WithDefaultPageSize(paginationConfig.DefaultPageSize),
			)
			if err != nil {
				return nil, err
			}

			return pointer.For(ledgercontroller.NewListLedgersQuery(pageSize)), nil
		})
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		ledgers, err := b.ListLedgers(r.Context(), *query)
		if err != nil {
			switch {
			case errors.Is(err, resources.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.RenderCursor(w, *ledgers)
	}
}
