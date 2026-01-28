package bulking

import (
	"fmt"
	"net/http"
	"slices"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

type JsonBulkHandler struct {
	bulkMaxSize  int
	bulkElements []*servicepb.LedgerAction
	receive      chan *servicepb.LedgerActionResult
}

func (h *JsonBulkHandler) GetChannels(w http.ResponseWriter, r *http.Request) (Bulk, chan *servicepb.LedgerActionResult, bool) {
	// Parse JSON array of bulk elements
	var rawElements []json.RawValue
	if err := json.UnmarshalRead(r.Body, &rawElements); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)
		return nil, nil, false
	}

	if h.bulkMaxSize != 0 && len(rawElements) > h.bulkMaxSize {
		writeErrorResponse(w, http.StatusRequestEntityTooLarge, "BULK_SIZE_EXCEEDED", fmt.Errorf("bulk size exceeded, max size is %d", h.bulkMaxSize))
		return nil, nil, false
	}

	// Parse each element using protobuf types
	h.bulkElements = make([]*servicepb.LedgerAction, 0, len(rawElements))
	for i, rawElem := range rawElements {
		elem := &servicepb.LedgerAction{}
		if err := json.Unmarshal(rawElem, elem); err != nil {
			writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", fmt.Errorf("error parsing element %d: %w", i, err))
			return nil, nil, false
		}
		h.bulkElements = append(h.bulkElements, elem)
	}

	bulk := make(Bulk, len(h.bulkElements))
	for _, element := range h.bulkElements {
		bulk <- element
	}
	close(bulk)

	h.receive = make(chan *servicepb.LedgerActionResult, len(h.bulkElements))

	return bulk, h.receive, true
}

func (h *JsonBulkHandler) Terminate(w http.ResponseWriter, _ *http.Request) {
	results := make([]*servicepb.LedgerActionResult, 0, len(h.bulkElements))
	for element := range h.receive {
		results = append(results, element)
	}

	actions := make([]string, len(h.bulkElements))
	for i, element := range h.bulkElements {
		actions[i] = servicepb.GetLedgerActionType(element)
	}
	writeJSONResponse(w, actions, results, nil)
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

func writeJSONResponse(w http.ResponseWriter, actions []string, results []*servicepb.LedgerActionResult, error error) {
	for _, result := range results {
		if HasError(result) {
			w.WriteHeader(http.StatusBadRequest)
			break
		}
	}

	slices.SortFunc(results, func(a, b *servicepb.LedgerActionResult) int {
		return int(a.ElementId) - int(b.ElementId)
	})

	mappedResults := make([]APIResult, 0)
	for _, result := range results {
		var (
			responseType = actions[result.ElementId]
			data         any
		)

		if HasError(result) {
			responseType = "ERROR"
		}

		// Extract data from oneof
		if log := result.GetLog(); log != nil {
			// For create transaction, return the transaction
			if log.Data != nil {
				if ct := log.Data.GetCreatedTransaction(); ct != nil {
					data = ct.Transaction
				}
			}
		}

		mappedResults = append(mappedResults, APIResult{
			ErrorCode:        result.ErrorCode,
			ErrorDescription: result.ErrorDescription,
			Data:             data,
			ResponseType:     responseType,
			LogID:            result.LogId,
		})
	}

	response := BulkResponse{
		Data: mappedResults,
	}
	if error != nil {
		response.ErrorCode = "VALIDATION"
		response.ErrorMessage = error.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, response); err != nil {
		panic(err)
	}
}

type BulkResponse struct {
	Data         []APIResult `json:"data,omitempty"`
	ErrorCode    string      `json:"errorCode,omitempty"`
	ErrorMessage string      `json:"errorMessage,omitempty"`
}

// writeErrorResponse writes an error response with the given status code, error code, and error
func writeErrorResponse(w http.ResponseWriter, statusCode int, errorCode string, err error) {
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}
	errorResp := struct {
		ErrorCode    string `json:"errorCode"`
		ErrorMessage string `json:"errorMessage"`
	}{
		ErrorCode:    errorCode,
		ErrorMessage: errorMsg,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.MarshalWrite(w, errorResp); err != nil {
		// If encoding fails, we can't write a proper error response
		return
	}
}
