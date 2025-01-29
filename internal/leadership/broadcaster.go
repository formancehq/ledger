package leadership

import (
	"sync"
)

type listener struct {
	channel chan Leadership
}

type Broadcaster struct {
	mu *sync.Mutex
	t  *Leadership

	inner []listener
	outer chan Leadership
}

func (h *Broadcaster) Actual() Leadership {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.t == nil {
		return Leadership{}
	}
	return *h.t
}

func (h *Broadcaster) Subscribe() (<-chan Leadership, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newChannel := make(chan Leadership, 1)
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

func (h *Broadcaster) Broadcast(t Leadership) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.t = &t

	for _, inner := range h.inner {
		inner.channel <- t
	}
}

func (h *Broadcaster) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, inner := range h.inner {
		close(inner.channel)
	}
}

func (h *Broadcaster) CountListeners() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.inner)
}

func NewSignal() *Broadcaster {
	return &Broadcaster{
		outer: make(chan Leadership),
		mu:    &sync.Mutex{},
	}
}
