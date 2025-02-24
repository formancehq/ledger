package bulking

import (
	"encoding/json"
	"net/http"
)

type JSONStreamBulkHandler struct {
	channel    Bulk
	terminated chan struct{}
	receive    chan BulkElementResult
	results    []BulkElementResult
	actions    []string
	err        error
}

func (h *JSONStreamBulkHandler) GetChannels(_ http.ResponseWriter, r *http.Request) (Bulk, chan BulkElementResult, bool) {

	h.channel = make(Bulk)
	h.receive = make(chan BulkElementResult)
	h.terminated = make(chan struct{})

	go func() {
		defer close(h.channel)

		dec := json.NewDecoder(r.Body)

		for {
			select {
			case <-r.Context().Done():
				return
			default:
				nextElement := &BulkElement{}
				err := dec.Decode(nextElement)
				if err != nil {
					h.err = err
					return
				}

				h.actions = append(h.actions, nextElement.GetAction())
				h.channel <- *nextElement
			}
		}
	}()
	go func() {
		defer close(h.terminated)

		for {
			select {
			case <-r.Context().Done():
				return
			case res, ok := <-h.receive:
				if !ok {
					return
				}
				h.results = append(h.results, res)
			}
		}
	}()

	return h.channel, h.receive, true
}

func (h *JSONStreamBulkHandler) Terminate(w http.ResponseWriter, r *http.Request) {
	select {
	case <-h.terminated:
		writeJSONResponse(w, h.actions, h.results, h.err)
	case <-r.Context().Done():
	}
}

func NewJSONStreamBulkHandler() *JSONStreamBulkHandler {
	return &JSONStreamBulkHandler{}
}

type JSONStreamBulkHandlerFactory struct{}

func (j JSONStreamBulkHandlerFactory) CreateBulkHandler() Handler {
	return NewJSONStreamBulkHandler()
}

func NewJSONStreamBulkHandlerFactory() HandlerFactory {
	return &JSONStreamBulkHandlerFactory{}
}

var _ HandlerFactory = (*JSONStreamBulkHandlerFactory)(nil)
