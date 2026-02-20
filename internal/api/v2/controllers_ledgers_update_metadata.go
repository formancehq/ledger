package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/metadata"

	"github.com/formancehq/ledger/internal/api/common"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

func updateLedgerMetadata(systemController systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody(w, r, func(m metadata.Metadata) {
			if err := systemController.UpdateLedgerMetadata(r.Context(), chi.URLParam(r, "ledger"), m); err != nil {
				common.HandleCommonWriteErrors(w, r, err)
				return
			}

			api.NoContent(w)
		})
	}
}
