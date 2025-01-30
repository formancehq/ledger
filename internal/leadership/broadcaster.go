package leadership

import (
	"sync"
)

type listener[T any] struct {
	channel chan T
}

type Broadcaster[T any] struct {
	mu sync.Mutex
	t  *T

	inner []listener[T]
	outer chan T
}

func (h *Broadcaster[T]) Actual() T {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.t == nil {
		var t T
		return t
	}
	return *h.t
}

func (h *Broadcaster[T]) Subscribe() (<-chan T, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newChannel := make(chan T, 1)
	l := listener[T]{
		channel: newChannel,
	}
	h.inner = append(h.inner, l)
	if h.t != nil {
		newChannel <- *h.t
	}

	return newChannel, func() {
		h.mu.Lock()
		defer h.mu.Unlock()

		for index, listener := range h.inner {
			if listener == l {
				if index < len(h.inner)-1 {
					h.inner = append(h.inner[:index], h.inner[index+1:]...)
				} else {
					h.inner = h.inner[:index]
				}
				break
			}
		}
	}
}

func (h *Broadcaster[T]) Broadcast(t T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.t = &t

	for _, inner := range h.inner {
		inner.channel <- t
	}
}

func (h *Broadcaster[T]) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, inner := range h.inner {
		close(inner.channel)
	}
}

func NewBroadcaster[T any]() *Broadcaster[T] {
	return &Broadcaster[T]{
		outer: make(chan T),
	}
}
