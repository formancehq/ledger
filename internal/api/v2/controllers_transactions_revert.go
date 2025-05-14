package v2

import (
	"net/http"
	"strconv"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func revertTransaction(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	txId, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	_, ret, err := l.RevertTransaction(
		r.Context(),
		getCommandParameters(r, ledgercontroller.RevertTransaction{
			Force:           api.QueryParamBool(r, "force"),
			AtEffectiveDate: api.QueryParamBool(r, "atEffectiveDate"),
			TransactionID:   txId,
		}),
	)
	if err != nil {
		switch {
		case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}):
			api.BadRequest(w, common.ErrInsufficientFund, err)
		case errors.Is(err, ledgercontroller.ErrAlreadyReverted{}):
			api.BadRequest(w, common.ErrAlreadyRevert, err)
		case errors.Is(err, ledgercontroller.ErrNotFound):
			api.NotFound(w, err)
		case errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Created(w, ret.RevertTransaction)
}
