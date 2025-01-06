package bulking

import "net/http"

type Handler interface {
	GetChannels(w http.ResponseWriter, r *http.Request) (Bulk, chan BulkElementResult, bool)
	Terminate(w http.ResponseWriter, r *http.Request)
}

type HandlerFactory interface {
	CreateBulkHandler() Handler
}