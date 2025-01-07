package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
)

func listPipelines() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		pipelines, err := l.ListPipelines(r.Context())
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.RenderCursor(w, *pipelines)
	}

}
