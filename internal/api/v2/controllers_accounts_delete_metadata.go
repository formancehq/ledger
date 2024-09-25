package v2

import (
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	if err := common.LedgerFromContext(r.Context()).
		DeleteAccountMetadata(
			r.Context(),
			getCommandParameters(r),
			param,
			chi.URLParam(r, "key"),
		); err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	api.NoContent(w)
}