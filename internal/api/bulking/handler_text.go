package bulking

import (
	"bufio"
	"net/http"
)

type ScriptStreamBulkHandler struct {
	channel    Bulk
	terminated chan struct{}
	receive    chan BulkElementResult
	results    []BulkElementResult
	actions    []string
	err        error
}

func (h *ScriptStreamBulkHandler) GetChannels(_ http.ResponseWriter, r *http.Request) (Bulk, chan BulkElementResult, bool) {

	h.channel = make(Bulk)
	h.receive = make(chan BulkElementResult)
	h.terminated = make(chan struct{})

	go func() {
		defer close(h.channel)

		scanner := bufio.NewScanner(r.Body)

		for {
			select {
			case <-r.Context().Done():
				return
			default:
				nextElement, err := ParseTextStream(scanner)
				if err != nil {
					h.err = err
					return
				}

				if nextElement == nil {
					// stream terminated
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

func (h *ScriptStreamBulkHandler) Terminate(w http.ResponseWriter, r *http.Request) {
	select {
	case <-h.terminated:
		writeJSONResponse(w, h.actions, h.results, h.err)
	case <-r.Context().Done():
	}
}

func NewScriptStreamBulkHandler() *ScriptStreamBulkHandler {
	return &ScriptStreamBulkHandler{}
}

type scriptStreamBulkHandlerFactory struct{}

func (j scriptStreamBulkHandlerFactory) CreateBulkHandler() Handler {
	return NewScriptStreamBulkHandler()
}

func NewScriptStreamBulkHandlerFactory() HandlerFactory {
	return &scriptStreamBulkHandlerFactory{}
}

var _ HandlerFactory = (*scriptStreamBulkHandlerFactory)(nil)
