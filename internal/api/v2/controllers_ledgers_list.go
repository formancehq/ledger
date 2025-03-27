package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/controller/system"
)

func listLedgers(b system.Controller, paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		query, err := bunpaginate.Extract[systemstore.ListLedgersQuery](r, func() (*systemstore.ListLedgersQuery, error) {
			pageSize, err := bunpaginate.GetPageSize(r,
				bunpaginate.WithMaxPageSize(paginationConfig.MaxPageSize),
				bunpaginate.WithDefaultPageSize(paginationConfig.DefaultPageSize),
			)
			if err != nil {
				return nil, err
			}

			return pointer.For(systemstore.NewListLedgersQuery(pageSize)), nil
		})
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		ledgers, err := b.ListLedgers(r.Context(), *query)
		if err != nil {
			switch {
			case errors.Is(err, ledgerstore.ErrInvalidQuery{}) || errors.Is(err, ledgerstore.ErrMissingFeature{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.RenderCursor(w, *ledgers)
	}
}
