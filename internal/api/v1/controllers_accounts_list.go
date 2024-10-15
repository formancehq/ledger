package v1

import (
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func listAccounts(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	query, err := bunpaginate.Extract[ledgercontroller.ListAccountsQuery](r, func() (*ledgercontroller.ListAccountsQuery, error) {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			return nil, err
		}
		options.QueryBuilder, err = buildAccountsFilterQuery(r)
		if err != nil {
			return nil, err
		}
		return pointer.For(ledgercontroller.NewListAccountsQuery(*options)), nil
	})
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.ListAccounts(r.Context(), *query)
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)
}
