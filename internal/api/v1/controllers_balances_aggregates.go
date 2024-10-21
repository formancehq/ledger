package v1

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func buildAggregatedBalancesQuery(r *http.Request) query.Builder {
	if address := r.URL.Query().Get("address"); address != "" {
		return query.Match("address", address)
	}

	return nil
}

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {

	pitFilter, err := getPITFilter(r)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	queryBuilder := buildAggregatedBalancesQuery(r)

	query := ledgercontroller.NewGetAggregatedBalancesQuery(*pitFilter, queryBuilder,
		// notes(gfyrag): if pit is not specified, always use insertion date to be backward compatible
		r.URL.Query().Get("pit") == "" || api.QueryParamBool(r, "useInsertionDate") || api.QueryParamBool(r, "use_insertion_date"))

	balances, err := common.LedgerFromContext(r.Context()).GetAggregatedBalances(r.Context(), query)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.Ok(w, balances)
}
