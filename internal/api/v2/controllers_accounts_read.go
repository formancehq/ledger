package v2

import (
	"github.com/formancehq/go-libs/v2/query"
	"net/http"
	"net/url"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/go-chi/chi/v5"
)

func readAccount(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, common.ErrValidation, err, err.Error())
		return
	}

	pit, err := getPIT(r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	acc, err := l.GetAccount(r.Context(), ledgercontroller.ResourceQuery[any]{
		PIT:     pit,
		Builder: query.Match("address", param),
		Expand: r.URL.Query()["expand"],
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

	api.Ok(w, acc)
}
