package v2

import (
	"errors"
	"github.com/formancehq/ledger/internal/api/bulking"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func bulkHandler(bulkerFactory bulking.BulkerFactory, bulkHandlerFactories map[string]bulking.HandlerFactory) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/json"
		}
		bulkHandlerFactory, ok := bulkHandlerFactories[contentType]
		if !ok {
			api.BadRequest(w, common.ErrValidation, errors.New("unsupported content type: "+contentType))
			return
		}

		bulkHandler := bulkHandlerFactory.CreateBulkHandler()
		send, receive, ok := bulkHandler.GetChannels(w, r)
		if !ok {
			return
		}

		l := common.LedgerFromContext(r.Context())

		err := bulkerFactory.CreateBulker(l).Run(r.Context(), send, receive,
			bulking.BulkingOptions{
				ContinueOnFailure: api.QueryParamBool(r, "continueOnFailure"),
				Atomic:            api.QueryParamBool(r, "atomic"),
				Parallel:          api.QueryParamBool(r, "parallel"),
			},
		)
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		bulkHandler.Terminate(w, r)
	}
}
