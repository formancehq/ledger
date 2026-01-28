package bulking

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

type Handler interface {
	GetChannels(w http.ResponseWriter, r *http.Request) (Bulk, chan *servicepb.LedgerActionResult, bool)
	Terminate(w http.ResponseWriter, r *http.Request)
}

type HandlerFactory interface {
	CreateBulkHandler() Handler
}
