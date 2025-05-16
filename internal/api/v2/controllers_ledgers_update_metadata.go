package v2

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/ledger/internal/api/common"

	"errors"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/metadata"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
)

func updateLedgerMetadata(systemController systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		m := metadata.Metadata{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			api.BadRequest(w, common.ErrValidation, errors.New("invalid format"))
			return
		}

		if err := systemController.UpdateLedgerMetadata(r.Context(), chi.URLParam(r, "ledger"), m); err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		api.NoContent(w)
	}
}
