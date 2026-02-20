package v2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/metadata"

	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func revertTransaction(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	txId, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	type request struct {
		Metadata metadata.Metadata `json:"metadata,omitempty"`
	}

	x := request{}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil {
			api.BadRequest(w, common.ErrValidation, errors.New("expected JSON body with metadata"))
			return
		}
	}

	_, ret, idempotencyHit, err := l.RevertTransaction(
		r.Context(),
		getCommandParameters(r, ledgercontroller.RevertTransaction{
			Force:           api.QueryParamBool(r, "force"),
			AtEffectiveDate: api.QueryParamBool(r, "atEffectiveDate"),
			TransactionID:   txId,
			Metadata:        x.Metadata,
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
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}
	if idempotencyHit {
		w.Header().Set("Idempotency-Hit", "true")
	}

	api.Created(w, renderTransaction(r, ret.RevertTransaction))
}
