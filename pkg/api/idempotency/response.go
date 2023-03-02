package idempotency

import (
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/logging"
)

type Response struct {
	RequestHash string
	StatusCode  int
	Header      http.Header
	Body        string
}

func (r Response) write(w http.ResponseWriter, req *http.Request) {
	for k, v := range r.Header {
		for _, vv := range v {
			w.Header().Add(k, vv)
		}
	}
	w.WriteHeader(r.StatusCode)
	if _, err := w.Write([]byte(r.Body)); err != nil {
		logging.FromContext(req.Context()).Errorf("Error writing stored response: %s", err)
	}
}
