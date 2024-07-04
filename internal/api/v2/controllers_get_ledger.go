package v2

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/sqlutils"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func getLedger(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		configuration := driver.LedgerState{}

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

		ledger, err := b.GetLedger(r.Context(), chi.URLParam(r, "ledger"))
		if err != nil {
			switch {
			case sqlutils.IsNotFoundError(err):
				sharedapi.NotFound(w, err)
			default:
				sharedapi.InternalServerError(w, r, err)
			}
			return
		}
		sharedapi.Ok(w, ledger)
	}
}
