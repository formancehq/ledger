package v1

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/sqlutils"
)

func autoCreateMiddleware(backend backend.Backend) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ledgerName := chi.URLParam(r, "ledger")
			if _, err := backend.GetLedger(r.Context(), ledgerName); err != nil {
				if !sqlutils.IsNotFoundError(err) {
					sharedapi.InternalServerError(w, r, err)
					return
				}

				if err := backend.CreateLedger(r.Context(), ledgerName, driver.LedgerConfiguration{
					Bucket: ledgerName,
				}); err != nil {
					sharedapi.InternalServerError(w, r, err)
					return
				}
			}

			handler.ServeHTTP(w, r)
		})
	}
}
