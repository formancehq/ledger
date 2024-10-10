package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/platform/postgres"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/metadata"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
)

func updateLedgerMetadata(systemController systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		m := metadata.Metadata{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			api.BadRequest(w, ErrValidation, errors.New("invalid format"))
			return
		}

		if err := systemController.UpdateLedgerMetadata(r.Context(), chi.URLParam(r, "ledger"), m); err != nil {
			switch {
			case errors.Is(err, postgres.ErrTooManyClient{}):
				api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.NoContent(w)
	}
}
