package v1

import (
	"errors"
	"github.com/formancehq/go-libs/platform/postgres"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func getStats(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	stats, err := l.GetStats(r.Context())
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrTooManyClient{}):
			api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.Ok(w, stats)
}
