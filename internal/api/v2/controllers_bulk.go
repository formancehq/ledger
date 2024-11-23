package v2

import (
	"encoding/json"
	"fmt"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/pointer"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func bulkHandler(bulkerFactory ledgercontroller.BulkerFactory, bulkMaxSize int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bulkElementDatas := make(JSONBulk, 0)
		if err := json.NewDecoder(r.Body).Decode(&bulkElementDatas); err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		if bulkMaxSize != 0 && len(bulkElementDatas) > bulkMaxSize {
			api.WriteErrorResponse(w, http.StatusRequestEntityTooLarge, ErrBulkSizeExceeded, fmt.Errorf("bulk size exceeded, max size is %d", bulkMaxSize))
			return
		}

		if err := bulkElementDatas.Validate(); err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		ledgerController := common.LedgerFromContext(r.Context())

		bulkElements := Map(bulkElementDatas, func(data ledgercontroller.BulkElementData) ledgercontroller.BulkElement {
			return ledgercontroller.BulkElement{
				Data:     data,
				Response: make(chan ledgercontroller.BulkElementResult, 1),
			}
		})
		bulk := make(ledgercontroller.Bulk, len(bulkElementDatas))
		for _, element := range bulkElements {
			bulk <- element
		}
		close(bulk)

		err := bulkerFactory.CreateBulker(ledgerController).Run(r.Context(), bulk,
			ledgercontroller.WithContinueOnFailure(api.QueryParamBool(r, "continueOnFailure")),
			ledgercontroller.WithAtomic(api.QueryParamBool(r, "atomic")),
			ledgercontroller.WithParallel(api.QueryParamBool(r, "parallel")),
		)
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		results := make([]ledgercontroller.BulkElementResult, 0, len(bulkElements))
		for _, element := range bulkElements {
			results = append(results, <-element.Response)
		}

		for _, result := range results {
			if result.Error != nil {
				w.WriteHeader(http.StatusBadRequest)
				break
			}
		}

		mappedResults := make([]Result, 0, len(bulkElements))
		for ind, result := range results {
			var (
				errorCode        string
				errorDescription string
				responseType     = bulkElementDatas[ind].Action
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

type JSONBulk []ledgercontroller.BulkElementData

func (b JSONBulk) Validate() error {
	for index, element := range b {
		switch element.Action {
		case ledgercontroller.ActionCreateTransaction:
			req := &ledgercontroller.TransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return fmt.Errorf("error parsing element %d: %s", index, err)
			}
		case ledgercontroller.ActionAddMetadata:
			req := &ledgercontroller.AddMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return fmt.Errorf("error parsing element %d: %s", index, err)
			}
		case ledgercontroller.ActionRevertTransaction:
			req := &ledgercontroller.RevertTransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return fmt.Errorf("error parsing element %d: %s", index, err)
			}
		case ledgercontroller.ActionDeleteMetadata:
			req := &ledgercontroller.DeleteMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return fmt.Errorf("error parsing element %d: %s", index, err)
			}
		}
	}

	return nil
}

type Result struct {
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorDescription string `json:"errorDescription,omitempty"`
	Data             any    `json:"data,omitempty"`
	ResponseType     string `json:"responseType"` // Added for sdk generation (discriminator in oneOf)
	LogID            int    `json:"logID"`
}
