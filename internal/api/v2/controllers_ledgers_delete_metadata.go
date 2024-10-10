package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/api"
	"github.com/go-chi/chi/v5"
)

func deleteLedgerMetadata(b system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := b.DeleteLedgerMetadata(r.Context(), chi.URLParam(r, "ledger"), chi.URLParam(r, "key")); err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.NoContent(w)
	}
}
