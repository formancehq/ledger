package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
)

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {
	options, err := getPaginatedQueryOptionsOfPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	balances, err := LedgerFromContext(r.Context()).
		GetAggregatedBalances(r.Context(), ledgerstore.NewGetAggregatedBalancesQuery(*options))
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, balances)
}
