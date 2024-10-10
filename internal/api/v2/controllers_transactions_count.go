package v2

import (
	"fmt"
	"github.com/formancehq/go-libs/platform/postgres"
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
		case errors.Is(err, postgres.ErrTooManyClient{}):
			api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	api.NoContent(w)
}
