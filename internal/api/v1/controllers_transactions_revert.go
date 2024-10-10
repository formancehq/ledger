package v1

import (
	"github.com/formancehq/go-libs/platform/postgres"
	"net/http"
	"strconv"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func revertTransaction(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	txId, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	ret, err := l.RevertTransaction(
		r.Context(),
		getCommandParameters(r, ledgercontroller.RevertTransaction{
			Force:           api.QueryParamBool(r, "disableChecks"),
			AtEffectiveDate: false,
			TransactionID:   int(txId),
		}),
	)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrTooManyClient{}):
			api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
		case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}):
			api.BadRequest(w, ErrInsufficientFund, err)
		case errors.Is(err, ledgercontroller.ErrAlreadyReverted{}):
			api.BadRequest(w, ErrAlreadyRevert, err)
		case errors.Is(err, ledgercontroller.ErrNotFound):
			api.NotFound(w, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.Created(w, mapTransactionToV1(ret.RevertTransaction))
}
