package bulking

import (
	"encoding/json/v2"
	"fmt"
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
	if err := json.UnmarshalRead(r.Body, &h.bulkElements); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)
		return nil, nil, false
	}

	if h.bulkMaxSize != 0 && len(h.bulkElements) > h.bulkMaxSize {
		writeErrorResponse(w, http.StatusRequestEntityTooLarge, "BULK_SIZE_EXCEEDED", fmt.Errorf("bulk size exceeded, max size is %d", h.bulkMaxSize))
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

	actions := make([]string, len(h.bulkElements))
	for i, element := range h.bulkElements {
		actions[i] = element.GetAction()
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
			errorCode = "ERROR"
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

