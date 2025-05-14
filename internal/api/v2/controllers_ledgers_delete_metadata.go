package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

func deleteLedgerMetadata(b system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := b.DeleteLedgerMetadata(r.Context(), chi.URLParam(r, "ledger"), chi.URLParam(r, "key")); err != nil {
			switch {
			case errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.NoContent(w)
	}
}
