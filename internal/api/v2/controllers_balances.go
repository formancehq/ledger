package v2

import (
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"net/http"

	"errors"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func readBalancesAggregated(w http.ResponseWriter, r *http.Request) {

	rq, err := getResourceQuery[ledgercontroller.GetAggregatedVolumesOptions](r, func(options *ledgercontroller.GetAggregatedVolumesOptions) error {
		options.UseInsertionDate = api.QueryParamBool(r, "use_insertion_date") || api.QueryParamBool(r, "useInsertionDate")

		return nil
	})
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	ctx := bundebug.WithDebug(r.Context())

	balances, err := common.LedgerFromContext(r.Context()).GetAggregatedBalances(ctx, *rq)
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, balances)
}
