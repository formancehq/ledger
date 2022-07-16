package httplistener

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/numary/ledger/pkg/bus"
)

var (
	server *httptest.Server
	mu     sync.Mutex
	events []string
)

func StartServer() {
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()

		events = append(events, string(data))
	}))
}

func StopServer() {
	server.Close()
}

func URL() string {
	return server.URL
}

func PickEvent[T bus.Payload](filter func(ledger string, payload T) bool) (*T, error) {
	mu.Lock()
	defer mu.Unlock()

	var zeroT T

	for _, eventStr := range events {
		e := &bus.Event[T]{}
		err := json.Unmarshal([]byte(eventStr), e)
		if err != nil {
			return nil, err
		}
		if e.Type != zeroT.PayloadType() {
			continue
		}
		if filter(e.Ledger, e.Payload) {
			return &e.Payload, nil
		}
	}

	return nil, nil
}
