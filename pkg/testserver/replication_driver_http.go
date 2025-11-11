package testserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/replication/drivers"
)

type HTTPDriver struct {
	srv       *httptest.Server
	collector *Collector
}

func (h *HTTPDriver) Clear(_ context.Context) error {
	h.collector.messages = nil
	return nil
}

func (h *HTTPDriver) Config() map[string]any {
	return map[string]any{
		"url": h.srv.URL,
	}
}

func (h *HTTPDriver) Name() string {
	return "http"
}

func (h *HTTPDriver) ReadMessages(_ context.Context) ([]drivers.LogWithLedger, error) {
	h.collector.mu.Lock()
	defer h.collector.mu.Unlock()

	return h.collector.messages[:], nil
}

var _ Driver = &HTTPDriver{}

func NewHTTPDriver(t interface {
	require.TestingT
	Cleanup(func())
}, collector *Collector) Driver {
	ret := &HTTPDriver{
		collector: collector,
	}

	ret.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newMessages := make([]drivers.LogWithLedger, 0)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&newMessages))

		ret.collector.mu.Lock()
		defer ret.collector.mu.Unlock()

		for _, message := range newMessages {
			exists := false
			for _, existingMessage := range ret.collector.messages {
				if existingMessage.ID == message.ID {
					exists = true
					break
				}
			}
			if !exists {
				ret.collector.messages = append(ret.collector.messages, message)
			}
		}

	}))
	t.Cleanup(ret.srv.Close)

	return ret
}

type Collector struct {
	mu       sync.Mutex
	messages []drivers.LogWithLedger
}

func NewCollector() *Collector {
	return &Collector{}
}
