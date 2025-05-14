package v2

import (
	"net/http"
	"net/url"

	"github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/go-chi/chi/v5"

	"errors"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	address, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, common.ErrValidation, err, err.Error())
		return
	}

	if _, err := common.LedgerFromContext(r.Context()).
		DeleteAccountMetadata(
			r.Context(),
			getCommandParameters(r, ledger.DeleteAccountMetadata{
				Address: address,
				Key:     chi.URLParam(r, "key"),
			}),
		); err != nil {
		switch {
		case errors.Is(err, ledger.ErrIdempotencyKeyConflict{}):
			api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
		case errors.Is(err, ledger.ErrInvalidIdempotencyInput{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.NoContent(w)
}
