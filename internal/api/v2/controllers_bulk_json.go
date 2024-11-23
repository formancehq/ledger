package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/api"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"
)

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

type jsonBulkHandler struct {
	bulkMaxSize  int
	bulkElements []ledgercontroller.BulkElement
}

func (h *jsonBulkHandler) GetChannel(w http.ResponseWriter, r *http.Request) ledgercontroller.Bulk {
	bulkElementDatas := make(JSONBulk, 0)
	if err := json.NewDecoder(r.Body).Decode(&bulkElementDatas); err != nil {
		api.BadRequest(w, ErrValidation, err)
		return nil
	}

	if h.bulkMaxSize != 0 && len(bulkElementDatas) > h.bulkMaxSize {
		api.WriteErrorResponse(w, http.StatusRequestEntityTooLarge, ErrBulkSizeExceeded, fmt.Errorf("bulk size exceeded, max size is %d", h.bulkMaxSize))
		return nil
	}

	if err := bulkElementDatas.Validate(); err != nil {
		api.BadRequest(w, ErrValidation, err)
		return nil
	}

	h.bulkElements = Map(bulkElementDatas, func(data ledgercontroller.BulkElementData) ledgercontroller.BulkElement {
		return ledgercontroller.BulkElement{
			Data:     data,
			Response: make(chan ledgercontroller.BulkElementResult, 1),
		}
	})
	bulk := make(ledgercontroller.Bulk, len(bulkElementDatas))
	for _, element := range h.bulkElements {
		bulk <- element
	}
	close(bulk)

	return bulk
}

func (h *jsonBulkHandler) Terminate(w http.ResponseWriter, _ *http.Request) {
	results := make([]ledgercontroller.BulkElementResult, 0, len(h.bulkElements))
	for _, element := range h.bulkElements {
		results = append(results, <-element.Response)
	}

	for _, result := range results {
		if result.Error != nil {
			w.WriteHeader(http.StatusBadRequest)
			break
		}
	}

	mappedResults := make([]Result, 0, len(h.bulkElements))
	for ind, result := range results {
		var (
			errorCode        string
			errorDescription string
			responseType     = h.bulkElements[ind].Data.Action
		)

		if result.Error != nil {
			switch {
			case errors.Is(result.Error, &ledgercontroller.ErrInsufficientFunds{}):
				errorCode = ErrInsufficientFund
			case errors.Is(result.Error, &ledgercontroller.ErrInvalidVars{}) || errors.Is(result.Error, ledgercontroller.ErrCompilationFailed{}):
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

type jsonBulkHandlerFactory struct {
	bulkMaxSize int
}

func (j jsonBulkHandlerFactory) CreateBulkHandler() BulkHandler {
	return &jsonBulkHandler{
		bulkMaxSize: j.bulkMaxSize,
	}
}

func NewJSONBulkHandlerFactory(bulkMaxSize int) BulkHandlerFactory {
	return &jsonBulkHandlerFactory{
		bulkMaxSize: bulkMaxSize,
	}
}

var _ BulkHandlerFactory = (*jsonBulkHandlerFactory)(nil)
