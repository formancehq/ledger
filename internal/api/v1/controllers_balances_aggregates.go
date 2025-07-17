package v1

import (
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/ledger/internal/api/common"
)

func buildAggregatedBalancesQuery(r *http.Request) query.Builder {
	if address := r.URL.Query().Get("address"); address != "" {
		return query.Match("address", address)
	}

	return nil
}

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {
	rq, err := getResourceQuery[ledgerstore.GetAggregatedVolumesOptions](r, func(q *ledgerstore.GetAggregatedVolumesOptions) error {
		q.UseInsertionDate = true

		return nil
	})
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	rq.Builder = buildAggregatedBalancesQuery(r)

	balances, err := common.LedgerFromContext(r.Context()).GetAggregatedBalances(r.Context(), *rq)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.Ok(w, balances)
}
