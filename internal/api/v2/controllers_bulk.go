package v2

import (
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v2/pointer"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func bulkHandler(bulkMaxSize int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b := ledgercontroller.Bulk{}
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		if bulkMaxSize != 0 && len(b) > bulkMaxSize {
			api.WriteErrorResponse(w, http.StatusRequestEntityTooLarge, ErrBulkSizeExceeded, fmt.Errorf("bulk size exceeded, max size is %d", bulkMaxSize))
			return
		}

		w.Header().Set("Content-Type", "application/json")

		ledgerController := common.LedgerFromContext(r.Context())
		bulker := ledgercontroller.NewBulker(ledgerController)
		results, err := bulker.Run(r.Context(),
			b,
			ledgercontroller.WithContinueOnFailure(api.QueryParamBool(r, "continueOnFailure")),
			ledgercontroller.WithAtomic(api.QueryParamBool(r, "atomic")),
		)
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}
		if results.HasErrors() {
			w.WriteHeader(http.StatusBadRequest)
		}

		mappedResults := make([]Result, 0)
		for ind, result := range results {
			var (
				errorCode        string
				errorDescription string
				responseType     = b[ind].Action
			)

			if result.Error != nil {
				switch {
				case errors.Is(result.Error, &ledgercontroller.ErrInsufficientFunds{}):
					errorCode = ErrInsufficientFund
				case errors.Is(result.Error, &ledgercontroller.ErrInvalidVars{}) || errors.Is(err, ledgercontroller.ErrCompilationFailed{}):
					errorCode = ErrCompilationFailed
				case errors.Is(result.Error, &ledgercontroller.ErrMetadataOverride{}):
					errorCode = ErrMetadataOverride
				case errors.Is(result.Error, ledgercontroller.ErrNoPostings):
					errorCode = ErrNoPostings
				case errors.Is(result.Error, ledgercontroller.ErrTransactionReferenceConflict{}):
					errorCode = ErrConflict
				case errors.Is(result.Error, ledgercontroller.ErrParsing{}):
					errorCode = ErrInterpreterParse
				case errors.Is(result.Error, ledgercontroller.ErrRuntime{}):
					errorCode = ErrInterpreterRuntime
				default:
					errorCode = api.ErrorInternal
				}
				errorDescription = result.Error.Error()
				responseType = "ERROR"
			}

			mappedResults = append(mappedResults, Result{
				ErrorCode:        errorCode,
				ErrorDescription: errorDescription,
				Data:             result.Data,
				ResponseType:     responseType,
				LogID:            result.LogID,
			})
		}

		if err := json.NewEncoder(w).Encode(api.BaseResponse[[]Result]{
			Data: pointer.For(mappedResults),
		}); err != nil {
			panic(err)
		}
	}
}

type Result struct {
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorDescription string `json:"errorDescription,omitempty"`
	Data             any    `json:"data,omitempty"`
	ResponseType     string `json:"responseType"` // Added for sdk generation (discriminator in oneOf)
	LogID            int    `json:"logID"`
}
