package v2

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/formancehq/ledger/internal/storage/driver"

	"github.com/formancehq/ledger/internal/api/backend"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func createLedger(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		configuration := driver.LedgerConfiguration{}

		data, err := io.ReadAll(r.Body)
		if err != nil && !errors.Is(err, io.EOF) {
			sharedapi.InternalServerError(w, r, err)
			return
		}

		if len(data) > 0 {
			if err := json.Unmarshal(data, &configuration); err != nil {
				sharedapi.BadRequest(w, ErrValidation, err)
				return
			}
		}

		if err := b.CreateLedger(r.Context(), chi.URLParam(r, "ledger"), configuration); err != nil {
			switch {
			case errors.Is(err, driver.ErrLedgerAlreadyExists):
				sharedapi.BadRequest(w, ErrValidation, err)
			default:
				sharedapi.InternalServerError(w, r, err)
			}
			return
		}
		sharedapi.NoContent(w)
	}
}
