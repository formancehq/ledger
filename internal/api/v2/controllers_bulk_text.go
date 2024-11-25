package v2

//
//import (
//	"bufio"
//	"encoding/json"
//	"errors"
//	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
//	"github.com/formancehq/ledger/internal/controller/ledger/bulking"
//	"github.com/formancehq/ledger/internal/machine/vm"
//	"net/http"
//	"strings"
//	"sync"
//)
//
//type textBulkHandler struct {
//	bulkElements []bulking.BulkElement
//	channel      bulking.Bulk
//	terminated   chan struct{}
//	wg           sync.WaitGroup
//}
//
///**
//package v2
//
//import (
//	"encoding/json"
//	"io"
//	"net/http"
//
//	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
//
//	"errors"
//	"github.com/formancehq/go-libs/v2/api"
//	ledger "github.com/formancehq/ledger/internal"
//	"github.com/formancehq/ledger/internal/api/common"
//)
//
//func importLogs(w http.ResponseWriter, r *http.Request) {
//
//	stream := make(chan ledger.Log)
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- common.LedgerFromContext(r.Context()).Import(r.Context(), stream)
//	}()
//	dec := json.NewDecoder(r.Body)
//	handleError := func(err error) {
//		switch {
//		case errors.Is(err, ledgercontroller.ErrImport{}):
//			api.BadRequest(w, "IMPORT", err)
//		default:
//			common.HandleCommonErrors(w, r, err)
//		}
//	}
//	for {
//		l := ledger.Log{}
//		if err := dec.Decode(&l); err != nil {
//			if errors.Is(err, io.EOF) {
//				close(stream)
//				break
//			} else {
//				api.InternalServerError(w, r, err)
//				return
//			}
//		}
//		select {
//		case stream <- l:
//		case <-r.Context().Done():
//			api.InternalServerError(w, r, r.Context().Err())
//			return
//		case err := <-errChan:
//			handleError(err)
//			return
//		}
//	}
//	select {
//	case err := <-errChan:
//		if err != nil {
//			handleError(err)
//			return
//		}
//	case <-r.Context().Done():
//		api.InternalServerError(w, r, r.Context().Err())
//		return
//	}
//
//	api.NoContent(w)
//}
//
//*/
//
//func (h *textBulkHandler) GetChannels(w http.ResponseWriter, r *http.Request) bulking.Bulk {
//
//	h.channel = make(bulking.Bulk)
//	h.terminated = make(chan struct{})
//	go func() {
//		defer close(h.terminated)
//
//		for {
//			select {
//			case <-r.Context().Done():
//				return
//			default:
//				nextElement, err := h.nextElement(r)
//				if err != nil {
//					// todo: handle error
//					return
//				}
//
//				h.wg.Add(1)
//
//				if nextElement == nil {
//					// stream terminated
//					return
//				}
//				ret := make(chan bulking.BulkElementResult, 1)
//				go func() {
//					// listen response
//					select {
//					case <-r.Context().Done():
//						return
//					case v := <-ret:
//						// todo: add a lock
//						if err := json.NewEncoder(w).Encode(v); err != nil {
//							// todo: handle error
//							return
//						}
//					}
//				}()
//
//				h.channel <- bulking.BulkElement{
//					Data:     *nextElement,
//					Response: ret,
//				}
//			}
//		}
//	}()
//
//	return h.channel
//}
//
//func (h *textBulkHandler) Terminate(_ http.ResponseWriter, r *http.Request) {
//	select {
//	case <-h.terminated:
//		h.wg.Wait()
//	case <-r.Context().Done():
//	}
//}
//
//func (h *textBulkHandler) nextElement(r *http.Request) (*bulking.BulkElement, error) {
//	scanner := bufio.NewScanner(r.Body)
//
//	// Read header
//	for scanner.Scan() {
//		text := scanner.Text()
//		switch {
//		case strings.HasPrefix(text, "//script:"):
//			bulkElementData := bulking.BulkElement{}
//			text = strings.TrimSuffix(text, "//script:")
//			text = strings.TrimSpace(text)
//			parts := strings.Split(text, ",")
//			for _, part := range parts {
//				parts2 := strings.Split(part, "=")
//				switch {
//				case parts2[0] == "ik":
//					bulkElementData.IdempotencyKey = parts2[1]
//				default:
//					return nil, errors.New("invalid header, key " + parts2[0] + " not recognized")
//				}
//			}
//
//			// Read body
//			plain := ""
//			for scanner.Scan() {
//				text = scanner.Text()
//				if text == "" {
//					bulkElementData.Data = bulking.TransactionRequest{
//						Script: ledgercontroller.ScriptV1{
//							Script: vm.Script{
//								Plain: plain,
//							},
//						},
//					}
//					return &bulkElementData, nil
//				}
//				plain += text + "\n"
//			}
//		case text == "":
//		default:
//			return nil, errors.New("invalid header")
//		}
//	}
//
//	return nil, nil
//}
//
//type textBulkHandlerFactory struct{}
//
//func (j textBulkHandlerFactory) CreateBulkHandler() BulkHandler {
//	return &textBulkHandler{}
//}
//
//func NewTextBulkHandlerFactory() BulkHandlerFactory {
//	return &textBulkHandlerFactory{}
//}
//
//var _ BulkHandlerFactory = (*textBulkHandlerFactory)(nil)
