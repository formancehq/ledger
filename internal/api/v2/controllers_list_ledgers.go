package v2

import (
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/ledger/internal/storage/systemstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"

	"github.com/formancehq/ledger/internal/api/backend"
)

func listLedgers(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		query := systemstore.ListLedgersQuery{}

		if r.URL.Query().Get(QueryKeyCursor) != "" {
			err := paginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &query)
			if err != nil {
				sharedapi.BadRequest(w, ErrValidation, fmt.Errorf("invalid '%s' query param", QueryKeyCursor))
				return
			}
		} else {
			pageSize, err := getPageSize(r)
			if err != nil {
				sharedapi.BadRequest(w, ErrValidation, err)
				return
			}

			query = systemstore.NewListLedgersQuery(pageSize)
		}

		ledgers, err := b.ListLedgers(r.Context(), query)
		if err != nil {
			sharedapi.InternalServerError(w, r, err)
			return
		}

		sharedapi.RenderCursor(w, *ledgers)
	}
}
