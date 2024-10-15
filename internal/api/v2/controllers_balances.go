package v2

import (
	"net/http"

	"errors"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func readBalancesAggregated(w http.ResponseWriter, r *http.Request) {

	pitFilter, err := getPITFilter(r)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	queryBuilder, err := getQueryBuilder(r)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	balances, err := common.LedgerFromContext(r.Context()).
		GetAggregatedBalances(r.Context(), ledgercontroller.NewGetAggregatedBalancesQuery(
			*pitFilter, queryBuilder, api.QueryParamBool(r, "use_insertion_date") || api.QueryParamBool(r, "useInsertionDate")))
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, balances)
}
