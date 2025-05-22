package common

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/ledger/internal/controller/system"

	"errors"
)

const (
	ErrOutdatedSchema = "OUTDATED_SCHEMA"
)

func LedgerMiddleware(
	backend system.Controller,
	resolver func(*http.Request) string,
	tracer trace.Tracer,
	excludePathFromSchemaCheck ...string,
) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := resolver(r)
			if name == "" {
				api.NotFound(w, errors.New("empty name"))
				return
			}

			ctx, span := tracer.Start(r.Context(), "OpenLedger", trace.WithAttributes(
				attribute.String("ledger", name),
			))
			defer span.End()

			var err error
			l, err := backend.GetLedgerController(ctx, name)
			if err != nil {
				switch {
				case postgres.IsNotFoundError(err):
					api.WriteErrorResponse(w, http.StatusNotFound, "LEDGER_NOT_FOUND", err)
				default:
					InternalServerError(w, r, err)
				}
				return
			}
			ctx = ContextWithLedger(ctx, l)

			pathWithoutLedger := r.URL.Path[1:]
			nextSlash := strings.Index(pathWithoutLedger, "/")
			if nextSlash >= 0 {
				pathWithoutLedger = pathWithoutLedger[nextSlash:]
			} else {
				pathWithoutLedger = ""
			}

			excluded := false
			for _, path := range excludePathFromSchemaCheck {
				if pathWithoutLedger == path {
					excluded = true
					break
				}
			}

			if !excluded {
				isUpToDate, err := l.IsDatabaseUpToDate(ctx)
				if err != nil {
					InternalServerError(w, r, err)
					return
				}
				if !isUpToDate {
					//nolint:staticcheck
					api.BadRequest(w, ErrOutdatedSchema, errors.New("You need to upgrade your ledger schema to the last version."))
					return
				}
			}

			handler.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
