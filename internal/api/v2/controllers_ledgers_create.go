package v2

import (
	"encoding/json"
	"github.com/formancehq/ledger/internal/api/common"
	"io"
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	ledger "github.com/formancehq/ledger/internal"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/go-chi/chi/v5"
)

func createLedger(systemController system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		configuration := ledger.Configuration{}
		data, err := io.ReadAll(r.Body)
		if err != nil && !errors.Is(err, io.EOF) {
			api.InternalServerError(w, r, err)
			return
		}

		if len(data) > 0 {
			if err := json.Unmarshal(data, &configuration); err != nil {
				api.BadRequest(w, common.ErrValidation, err)
				return
			}
		}

		if err := systemController.CreateLedger(r.Context(), chi.URLParam(r, "ledger"), configuration); err != nil {
			switch {
			case errors.Is(err, system.ErrInvalidLedgerConfiguration{}) ||
				errors.Is(err, ledger.ErrInvalidLedgerName{}) ||
				errors.Is(err, ledger.ErrInvalidBucketName{}):
				api.BadRequest(w, common.ErrValidation, err)
			case errors.Is(err, system.ErrBucketOutdated):
				api.BadRequest(w, common.ErrOutdatedSchema, err)
			case errors.Is(err, system.ErrLedgerAlreadyExists):
				api.BadRequest(w, common.ErrLedgerAlreadyExists, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}
		api.NoContent(w)
	}
}
