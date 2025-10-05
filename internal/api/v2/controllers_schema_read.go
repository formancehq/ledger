package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/platform/postgres"

	"github.com/formancehq/ledger/internal/api/common"
)

func readSchema(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	version := chi.URLParam(r, "version")
	schema, err := l.GetSchema(r.Context(), version)
	if err != nil {
		switch {
		case postgres.IsNotFoundError(err):
			api.NotFound(w, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, schema)
}
