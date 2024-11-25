package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"
	"slices"
)

type jsonBulkHandler struct {
	bulkMaxSize  int
	bulkElements []ledgercontroller.BulkElement
	receive      chan ledgercontroller.BulkElementResult
}

func (h *jsonBulkHandler) GetChannels(w http.ResponseWriter, r *http.Request) (ledgercontroller.Bulk, chan ledgercontroller.BulkElementResult, bool) {
	h.bulkElements = make([]ledgercontroller.BulkElement, 0)
	if err := json.NewDecoder(r.Body).Decode(&h.bulkElements); err != nil {
		api.BadRequest(w, ErrValidation, err)
		return nil, nil, false
	}

	if h.bulkMaxSize != 0 && len(h.bulkElements) > h.bulkMaxSize {
		api.WriteErrorResponse(w, http.StatusRequestEntityTooLarge, ErrBulkSizeExceeded, fmt.Errorf("bulk size exceeded, max size is %d", h.bulkMaxSize))
		return nil, nil, false
	}

	bulk := make(ledgercontroller.Bulk, len(h.bulkElements))
	for _, element := range h.bulkElements {
		bulk <- element
	}
	close(bulk)

	h.receive = make(chan ledgercontroller.BulkElementResult, len(h.bulkElements))

	return bulk, h.receive, true
}

func (h *jsonBulkHandler) Terminate(w http.ResponseWriter, _ *http.Request) {
	results := make([]ledgercontroller.BulkElementResult, 0, len(h.bulkElements))
	for element := range h.receive {
		results = append(results, element)
	}

	for _, result := range results {
		if result.Error != nil {
			w.WriteHeader(http.StatusBadRequest)
			break
		}
	}

	slices.SortFunc(results, func(a, b ledgercontroller.BulkElementResult) int {
		return a.ElementID - b.ElementID
	})

	mappedResults := make([]Result, 0, len(h.bulkElements))
	for ind, result := range results {
		var (
			errorCode        string
			errorDescription string
			responseType     = h.bulkElements[ind].Action
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
