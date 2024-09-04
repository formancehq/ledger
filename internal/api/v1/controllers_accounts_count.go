package v1

import (
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
)

func countAccounts(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	query, err := bunpaginate.Extract[ledgercontroller.ListAccountsQuery](r, func() (*ledgercontroller.ListAccountsQuery, error) {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			return nil, err
		}
		options.QueryBuilder, err = buildAccountsFilterQuery(r)
		return pointer.For(ledgercontroller.NewListAccountsQuery(*options)), nil
	})
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := l.CountAccounts(r.Context(), *query)
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	api.NoContent(w)
}
