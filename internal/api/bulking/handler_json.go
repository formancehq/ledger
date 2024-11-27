package bulking

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"net/http"
	"slices"
)

type JsonBulkHandler struct {
	bulkMaxSize  int
	bulkElements []BulkElement
	receive      chan BulkElementResult
}

func (h *JsonBulkHandler) GetChannels(w http.ResponseWriter, r *http.Request) (Bulk, chan BulkElementResult, bool) {
	h.bulkElements = make([]BulkElement, 0)
	if err := json.NewDecoder(r.Body).Decode(&h.bulkElements); err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return nil, nil, false
	}

	if h.bulkMaxSize != 0 && len(h.bulkElements) > h.bulkMaxSize {
		api.WriteErrorResponse(w, http.StatusRequestEntityTooLarge, common.ErrBulkSizeExceeded, fmt.Errorf("bulk size exceeded, max size is %d", h.bulkMaxSize))
		return nil, nil, false
	}

	bulk := make(Bulk, len(h.bulkElements))
	for _, element := range h.bulkElements {
		bulk <- element
	}
	close(bulk)

	h.receive = make(chan BulkElementResult, len(h.bulkElements))

	return bulk, h.receive, true
}

func (h *JsonBulkHandler) Terminate(w http.ResponseWriter, _ *http.Request) {
	results := make([]BulkElementResult, 0, len(h.bulkElements))
	for element := range h.receive {
		results = append(results, element)
	}

	writeJSONResponse(w, collectionutils.Map(h.bulkElements, BulkElement.GetAction), results, nil)
}

func NewJSONBulkHandler(bulkMaxSize int) *JsonBulkHandler {
	return &JsonBulkHandler{
		bulkMaxSize: bulkMaxSize,
	}
}

type jsonBulkHandlerFactory struct {
	bulkMaxSize int
}

func (j jsonBulkHandlerFactory) CreateBulkHandler() Handler {
	return NewJSONBulkHandler(j.bulkMaxSize)
}

func NewJSONBulkHandlerFactory(bulkMaxSize int) HandlerFactory {
	return &jsonBulkHandlerFactory{
		bulkMaxSize: bulkMaxSize,
	}
}

var _ HandlerFactory = (*jsonBulkHandlerFactory)(nil)

func writeJSONResponse(w http.ResponseWriter, actions []string, results []BulkElementResult, error error) {
	for _, result := range results {
		if result.Error != nil {
			w.WriteHeader(http.StatusBadRequest)
			break
		}
	}

	slices.SortFunc(results, func(a, b BulkElementResult) int {
		return a.ElementID - b.ElementID
	})

	mappedResults := make([]APIResult, 0)
	for index, result := range results {
		var (
			errorCode        string
			errorDescription string
			responseType     = actions[index]
		)

		if result.Error != nil {
			switch {
			case errors.Is(result.Error, &ledgercontroller.ErrInsufficientFunds{}):
				errorCode = common.ErrInsufficientFund
			case errors.Is(result.Error, &ledgercontroller.ErrInvalidVars{}) || errors.Is(result.Error, ledgercontroller.ErrCompilationFailed{}):
				errorCode = common.ErrCompilationFailed
			case errors.Is(result.Error, &ledgercontroller.ErrMetadataOverride{}):
				errorCode = common.ErrMetadataOverride
			case errors.Is(result.Error, ledgercontroller.ErrNoPostings):
				errorCode = common.ErrNoPostings
			case errors.Is(result.Error, ledgercontroller.ErrTransactionReferenceConflict{}):
				errorCode = common.ErrConflict
			case errors.Is(result.Error, ledgercontroller.ErrParsing{}):
				errorCode = common.ErrInterpreterParse
			case errors.Is(result.Error, ledgercontroller.ErrRuntime{}):
				errorCode = common.ErrInterpreterRuntime
			default:
				errorCode = api.ErrorInternal
			}
			errorDescription = result.Error.Error()
			responseType = "ERROR"
		}

		mappedResults = append(mappedResults, APIResult{
			ErrorCode:        errorCode,
			ErrorDescription: errorDescription,
			Data:             result.Data,
			ResponseType:     responseType,
			LogID:            result.LogID,
		})
	}

	if err := json.NewEncoder(w).Encode(ComposedErrorResponse{
		BaseResponse: api.BaseResponse[[]APIResult]{
			Data: pointer.For(mappedResults),
		},
		ErrorResponse: func() api.ErrorResponse {
			ret := api.ErrorResponse{}
			if error != nil {
				ret.ErrorCode = common.ErrValidation
				ret.ErrorMessage = error.Error()
			}
			return ret
		}(),
	}); err != nil {
		panic(err)
	}
}

type ComposedErrorResponse struct {
	api.BaseResponse[[]APIResult]
	api.ErrorResponse
}
