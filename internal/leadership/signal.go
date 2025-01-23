package leadership

import (
	"github.com/formancehq/go-libs/v2/pointer"
	"sync"
)

type listener struct {
	channel chan bool
}

type Signal struct {
	mu *sync.Mutex
	t  *bool

	inner []listener
	outer chan bool
}

func (h *Signal) Actual() *bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.t == nil {
		return nil
	}

	return pointer.For(*h.t)
}

func (h *Signal) Listen() (<-chan bool, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newChannel := make(chan bool, 1)
	index := len(h.inner)
	h.inner = append(h.inner, listener{
		channel: newChannel,
	})
	if h.t != nil {
		newChannel <- *h.t
	}

	return newChannel, func() {
		h.mu.Lock()
		defer h.mu.Unlock()

		if index < len(h.inner)-1 {
			h.inner = append(h.inner[:index], h.inner[index+1:]...)
		} else {
			h.inner = h.inner[:index]
		}
	}
}

func (h *Signal) Signal(t bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.t = &t

	for _, inner := range h.inner {
		inner.channel <- t
	}
}

func (h *Signal) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, inner := range h.inner {
		close(inner.channel)
	}
}

func (h *Signal) CountListeners() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.inner)
}

func NewSignal() *Signal {
	return &Signal{
		outer: make(chan bool),
		mu:    &sync.Mutex{},
	}
}
