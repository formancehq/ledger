package v1

import (
	"errors"
	"github.com/formancehq/go-libs/platform/postgres"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func buildAggregatedBalancesQuery(r *http.Request) (query.Builder, error) {
	if address := r.URL.Query().Get("address"); address != "" {
		return query.Match("address", address), nil
	}

	return nil, nil
}

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {

	pitFilter, err := getPITFilter(r)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	queryBuilder, err := buildAggregatedBalancesQuery(r)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	query := ledgercontroller.NewGetAggregatedBalancesQuery(*pitFilter, queryBuilder,
		// notes(gfyrag): if pit is not specified, always use insertion date to be backward compatible
		r.URL.Query().Get("pit") == "" || api.QueryParamBool(r, "useInsertionDate") || api.QueryParamBool(r, "use_insertion_date"))

	balances, err := common.LedgerFromContext(r.Context()).GetAggregatedBalances(r.Context(), query)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrTooManyClient{}):
			api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.Ok(w, balances)
}
