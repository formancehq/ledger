package v2

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func bulkHandler(bulkerFactory ledgercontroller.BulkerFactory, bulkHandlerFactories map[string]BulkHandlerFactory) http.HandlerFunc {
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
		channel := bulkHandler.GetChannel(w, r)
		if channel == nil {
			return
		}

		l := common.LedgerFromContext(r.Context())

		err := bulkerFactory.CreateBulker(l).Run(r.Context(), channel,
			ledgercontroller.WithContinueOnFailure(api.QueryParamBool(r, "continueOnFailure")),
			ledgercontroller.WithAtomic(api.QueryParamBool(r, "atomic")),
			ledgercontroller.WithParallel(api.QueryParamBool(r, "parallel")),
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
	GetChannel(w http.ResponseWriter, r *http.Request) ledgercontroller.Bulk
	Terminate(w http.ResponseWriter, r *http.Request)
}

type BulkHandlerFactory interface {
	CreateBulkHandler() BulkHandler
}