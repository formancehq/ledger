package v2

import (
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func listTransactions(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	paginationColumn := "id"
	if r.URL.Query().Get("order") == "effective" {
		paginationColumn = "timestamp"
	}

	rq, err := getColumnPaginatedQuery[any](r, paginationColumn, bunpaginate.OrderDesc)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	cursor, err := l.ListTransactions(r.Context(), *rq)
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)
}
