package bulking

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

type Handler interface {
	GetChannels(w http.ResponseWriter, r *http.Request) (Bulk, chan *ledgerpb.LedgerActionResult, bool)
	Terminate(w http.ResponseWriter, r *http.Request)
}

type HandlerFactory interface {
	CreateBulkHandler() Handler
}
