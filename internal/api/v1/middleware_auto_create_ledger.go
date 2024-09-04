package v1

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/api"
	"github.com/go-chi/chi/v5"
)

func autoCreateMiddleware(backend system.Controller) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ledgerName := chi.URLParam(r, "ledger")
			if _, err := backend.GetLedger(r.Context(), ledgerName); err != nil {
				if !postgres.IsNotFoundError(err) {
					api.InternalServerError(w, r, err)
					return
				}

				if err := backend.CreateLedger(r.Context(), ledgerName, ledger.Configuration{
					Bucket: ledgerName,
				}); err != nil {
					switch {
					case errors.Is(err, ledger.ErrInvalidLedgerName{}):
						api.BadRequest(w, ErrValidation, err)
					default:
						api.InternalServerError(w, r, err)
					}
					return
				}
			}

			handler.ServeHTTP(w, r)
		})
	}
}
