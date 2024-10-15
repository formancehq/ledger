package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func readStats(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	stats, err := l.GetStats(r.Context())
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.Ok(w, stats)
}
