package v2

import (
	"errors"
	"github.com/formancehq/ledger/internal/bulking"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func bulkHandler(bulkerFactory bulking.BulkerFactory, bulkHandlerFactories map[string]BulkHandlerFactory) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/json"
		}
		bulkHandlerFactory, ok := bulkHandlerFactories[contentType]
		if !ok {
			api.BadRequest(w, ErrValidation, errors.New("unsupported content type: "+contentType))
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

type Result struct {
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorDescription string `json:"errorDescription,omitempty"`
	Data             any    `json:"data,omitempty"`
	ResponseType     string `json:"responseType"` // Added for sdk generation (discriminator in oneOf)
	LogID            int    `json:"logID"`
}

type BulkHandler interface {
	GetChannels(w http.ResponseWriter, r *http.Request) (bulking.Bulk, chan bulking.BulkElementResult, bool)
	Terminate(w http.ResponseWriter, r *http.Request)
}

type BulkHandlerFactory interface {
	CreateBulkHandler() BulkHandler
}
