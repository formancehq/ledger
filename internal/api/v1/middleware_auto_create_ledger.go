package v1

import (
	"github.com/formancehq/ledger/internal/api/common"
	"go.opentelemetry.io/otel/trace"
	"net/http"

	"errors"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/go-chi/chi/v5"
)

func autoCreateMiddleware(backend system.Controller, tracer trace.Tracer) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ctx, span := tracer.Start(r.Context(), "AutomaticLedgerCreate")
			defer span.End()

			ledgerName := chi.URLParam(r, "ledger")
			if _, err := backend.GetLedger(ctx, ledgerName); err != nil {
				if !postgres.IsNotFoundError(err) {
					common.InternalServerError(w, r, err)
					return
				}

				if err := backend.CreateLedger(ctx, ledgerName, ledger.Configuration{
					Bucket: ledgerName,
				}); err != nil {
					switch {
					case errors.Is(err, ledger.ErrInvalidLedgerName{}):
						api.BadRequest(w, common.ErrValidation, err)
					default:
						common.InternalServerError(w, r, err)
					}
					return
				}
			}
			span.End()

			handler.ServeHTTP(w, r)
		})
	}
}
