package v2

import (
	"net/http"
	"net/url"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/go-chi/chi/v5"
)

func readAccount(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	query := ledgercontroller.NewGetAccountQuery(param)
	if hasExpandVolumes(r) {
		query = query.WithExpandVolumes()
	}
	if hasExpandEffectiveVolumes(r) {
		query = query.WithExpandEffectiveVolumes()
	}
	pitFilter, err := getPITFilter(r)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}
	query.PITFilter = *pitFilter

	acc, err := l.GetAccount(r.Context(), query)
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
