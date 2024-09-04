package v2

import (
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"

	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/bun/bunpaginate"
)

func readVolumes(w http.ResponseWriter, r *http.Request) {

	l := common.LedgerFromContext(r.Context())

	query, err := bunpaginate.Extract[ledgercontroller.GetVolumesWithBalancesQuery](r, func() (*ledgercontroller.GetVolumesWithBalancesQuery, error) {
		options, err := getPaginatedQueryOptionsOfFiltersForVolumes(r)
		if err != nil {
			return nil, err
		}

		getVolumesWithBalancesQuery := ledgercontroller.NewGetVolumesWithBalancesQuery(*options)
		return pointer.For(getVolumesWithBalancesQuery), nil

	})

	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.GetVolumesWithBalances(r.Context(), *query)

	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)

}
