package v2

import (
	"github.com/formancehq/go-libs/v3/query"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func readTransaction(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	txId, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	pit, err := getPIT(r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	tx, err := l.GetTransaction(r.Context(), storagecommon.ResourceQuery[any]{
		PIT:     pit,
		Builder: query.Match("id", int(txId)),
		Expand:  r.URL.Query()["expand"],
	})
	if err != nil {
		switch {
		case postgres.IsNotFoundError(err):
			api.NotFound(w, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, tx)
}
