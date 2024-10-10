package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/platform/postgres"

	"github.com/formancehq/go-libs/api"
	"github.com/go-chi/chi/v5"
)

func readLedger(b system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ledger, err := b.GetLedger(r.Context(), chi.URLParam(r, "ledger"))
		if err != nil {
			switch {
			case postgres.IsNotFoundError(err):
				api.NotFound(w, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}
		api.Ok(w, ledger)
	}
}
