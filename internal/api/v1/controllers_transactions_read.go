package v1

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/query"

	"github.com/formancehq/ledger/internal/api/common"
)

func readTransaction(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	txId, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	rq, err := getResourceQuery[any](r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}
	rq.Builder = query.Match("id", txId)

	tx, err := l.GetTransaction(r.Context(), *rq)
	if err != nil {
		switch {
		case postgres.IsNotFoundError(err):
			api.NotFound(w, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, mapTransactionToV1(*tx))
}
