package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/api/shared"

	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
)

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {
	options, err := getPaginatedQueryOptionsOfPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, shared.ErrValidation, err)
		return
	}

	balances, err := shared.LedgerFromContext(r.Context()).
		GetAggregatedBalances(r.Context(), ledgerstore.NewGetAggregatedBalancesQuery(*options))
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, balances)
}
