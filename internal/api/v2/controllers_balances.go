package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
)

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {

	pitFilter, err := getPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	queryBuilder, err := getQueryBuilder(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	balances, err := backend.LedgerFromContext(r.Context()).
		GetAggregatedBalances(r.Context(), ledgerstore.NewGetAggregatedBalancesQuery(
			*pitFilter, queryBuilder, sharedapi.QueryParamBool(r, "use_insertion_date") || sharedapi.QueryParamBool(r, "useInsertionDate")))
	if err != nil {
		switch {
		case ledgerstore.IsErrInvalidQuery(err):
			sharedapi.BadRequest(w, ErrValidation, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	sharedapi.Ok(w, balances)
}
