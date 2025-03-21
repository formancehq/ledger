package v2

import (
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func readBalancesAggregated(w http.ResponseWriter, r *http.Request) {

	rq, err := getResourceQuery[ledgerstore.GetAggregatedVolumesOptions](r, func(options *ledgerstore.GetAggregatedVolumesOptions) error {
		options.UseInsertionDate = api.QueryParamBool(r, "use_insertion_date") || api.QueryParamBool(r, "useInsertionDate")

		return nil
	})
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	balances, err := common.LedgerFromContext(r.Context()).GetAggregatedBalances(r.Context(), *rq)
	if err != nil {
		switch {
		case errors.Is(err, ledgerstore.ErrInvalidQuery{}) || errors.Is(err, ledgerstore.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, balances)
}
