package v2

import (
	"fmt"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func countTransactions(w http.ResponseWriter, r *http.Request) {

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := common.LedgerFromContext(r.Context()).
		CountTransactions(r.Context(), ledgercontroller.NewListTransactionsQuery(*options))
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	api.NoContent(w)
}
