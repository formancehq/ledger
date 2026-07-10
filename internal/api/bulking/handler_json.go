package bulking

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/types/collections"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/numscript"

	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
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

	writeJSONResponse(w, collections.Map(h.bulkElements, BulkElement.GetAction), results, nil)
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
			errorCode = mapBulkElementError(result.Error)
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

// mapBulkElementError maps a controller error for a bulk element to the same API
// error code the individual (non-bulk) endpoints return, so a business error in a
// bulk is not reported as a generic INTERNAL. It must stay consistent with the
// per-endpoint mappings in internal/api/v2 and common.HandleCommon*Errors.
func mapBulkElementError(err error) string {
	switch {
	case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}), errors.Is(err, numscript.MissingFundsErr{}):
		return common.ErrInsufficientFund
	case errors.Is(err, &ledgercontroller.ErrInvalidVars{}) || errors.Is(err, ledgercontroller.ErrCompilationFailed{}):
		return common.ErrCompilationFailed
	case errors.Is(err, &ledgercontroller.ErrMetadataOverride{}):
		return common.ErrMetadataOverride
	case errors.Is(err, ledgercontroller.ErrNoPostings):
		return common.ErrNoPostings
	case errors.Is(err, ledgerstore.ErrTransactionReferenceConflict{}), errors.Is(err, ledgercontroller.ErrIdempotencyKeyConflict{}):
		return common.ErrConflict
	case errors.Is(err, ledgercontroller.ErrParsing{}):
		return common.ErrInterpreterParse
	case errors.Is(err, ledgercontroller.ErrRuntime{}):
		return common.ErrInterpreterRuntime
	case errors.Is(err, ledgercontroller.ErrAlreadyReverted{}):
		return common.ErrAlreadyRevert
	case errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}), errors.Is(err, ledgercontroller.ErrSchemaValidationError{}):
		return common.ErrValidation
	case errors.Is(err, ledgercontroller.ErrSchemaNotSpecified{}):
		return common.ErrSchemaNotSpecified
	case errors.Is(err, ledgercontroller.ErrNotFound), errors.Is(err, ledgercontroller.ErrSchemaNotFound{}):
		return api.ErrorCodeNotFound
	default:
		return api.ErrorInternal
	}
}
