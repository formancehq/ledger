package v2

import (
	"bufio"
	"encoding/json"
	"errors"
	"github.com/formancehq/ledger/internal/bulking"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/machine/vm"
	"net/http"
	"strings"
	"sync"
)

type textBulkHandler struct {
	channel    bulking.Bulk
	terminated chan struct{}
	wg         sync.WaitGroup
	receive    chan bulking.BulkElementResult
}

func (h *textBulkHandler) GetChannels(w http.ResponseWriter, r *http.Request) (bulking.Bulk, chan bulking.BulkElementResult, bool) {

	h.channel = make(bulking.Bulk)
	h.receive = make(chan bulking.BulkElementResult)
	h.terminated = make(chan struct{})
	go func() {
		defer close(h.terminated)
		defer close(h.receive)

		for {
			select {
			case <-r.Context().Done():
				return
			default:
				nextElement, err := h.nextElement(r)
				if err != nil {
					// todo: handle error
					return
				}

				h.wg.Add(1)

				if nextElement == nil {
					// stream terminated
					return
				}

				h.channel <- *nextElement
			}
		}
	}()
	go func() {
		for {
			select {
			case <-r.Context().Done():
				return
			case res := <-h.receive:
				if err := json.NewEncoder(w).Encode(res); err != nil {
					// todo: handle error
				}
			}
		}
	}()

	return h.channel, h.receive, true
}

func (h *textBulkHandler) Terminate(_ http.ResponseWriter, r *http.Request) {
	select {
	case <-h.terminated:
		h.wg.Wait()
	case <-r.Context().Done():
	}
}

func (h *textBulkHandler) nextElement(r *http.Request) (*bulking.BulkElement, error) {
	scanner := bufio.NewScanner(r.Body)

	// Read header
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		switch {
		case text == "":
		case strings.HasPrefix(text, "//script:"):
			bulkElementData := bulking.BulkElement{}
			text = strings.TrimSuffix(text, "//script:")
			text = strings.TrimSpace(text)
			parts := strings.Split(text, ",")
			for _, part := range parts {
				parts2 := strings.Split(part, "=")
				switch {
				case parts2[0] == "ik":
					bulkElementData.IdempotencyKey = parts2[1]
				default:
					return nil, errors.New("invalid header, key " + parts2[0] + " not recognized")
				}
			}

			// Read body
			plain := ""
			for scanner.Scan() {
				text = strings.TrimSpace(scanner.Text())
				if text == "//end" {
					bulkElementData.Data = bulking.TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: vm.Script{
								Plain: plain,
							},
						},
					}
					return &bulkElementData, nil
				}
				plain += text + "\n"
			}
		default:
			return nil, errors.New("invalid header")
		}
	}

	return nil, nil
}

type textBulkHandlerFactory struct{}

func (j textBulkHandlerFactory) CreateBulkHandler() BulkHandler {
	return &textBulkHandler{}
}

func NewTextBulkHandlerFactory() BulkHandlerFactory {
	return &textBulkHandlerFactory{}
}

var _ BulkHandlerFactory = (*textBulkHandlerFactory)(nil)
