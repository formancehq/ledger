package v2

import (
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/api"
)

func deletePipeline() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())
		if err := l.DeletePipeline(r.Context(), getPipelineID(r)); err != nil {
			switch {
			case errors.Is(err, ledgercontroller.ErrPipelineNotFound("")):
				api.NotFound(w, err)
			case errors.Is(err, ledgercontroller.ErrInUsePipeline("")):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}

}
