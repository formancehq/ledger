package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/api"
)

func readPipeline() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		pipeline, err := l.GetPipeline(r.Context(), getPipelineID(r))
		if err != nil {
			switch {
			case errors.Is(err, ledgercontroller.ErrPipelineNotFound("")):
				api.NotFound(w, err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		api.Ok(w, pipeline)
	}

}
