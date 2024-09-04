package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func readStats(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	stats, err := l.Stats(r.Context())
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	api.Ok(w, stats)
}
