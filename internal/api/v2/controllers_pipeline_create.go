package v2

import (
	ingester "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v2/api"
)

func createPipeline() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[ingester.PipelineConfiguration](w, r, func(req ingester.PipelineConfiguration) {
			l := common.LedgerFromContext(r.Context())

			p, err := l.CreatePipeline(r.Context(), req)
			if err != nil {
				switch {
				case errors.Is(err, systemcontroller.ErrConnectorNotFound("")) ||
					errors.Is(err, ledgercontroller.ErrPipelineAlreadyExists{}) ||
					errors.Is(err, ledgercontroller.ErrInUsePipeline("")):
					api.BadRequest(w, "VALIDATION", err)
				default:
					api.InternalServerError(w, r, err)
				}
				return
			}

			api.Created(w, p)
		})
	}
}
