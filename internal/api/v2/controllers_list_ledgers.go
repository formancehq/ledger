package v2

import (
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/pointer"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/storage/systemstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"

	"github.com/formancehq/ledger/internal/api/backend"
)

func listLedgers(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		query, err := bunpaginate.Extract[systemstore.ListLedgersQuery](r, func() (*systemstore.ListLedgersQuery, error) {
			pageSize, err := bunpaginate.GetPageSize(r)
			if err != nil {
				return nil, err
			}

			return pointer.For(systemstore.NewListLedgersQuery(pageSize)), nil
		})
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, err)
			return
		}

		ledgers, err := b.ListLedgers(r.Context(), *query)
		if err != nil {
			sharedapi.InternalServerError(w, r, err)
			return
		}

		sharedapi.RenderCursor(w, *ledgers)
	}
}
