package runner

import (
	"encoding/json"
	"sync"
)

type listener[T comparable] struct {
	channel chan T
	filters []ChangerFilter[T]
}

type ChangerFilter[T comparable] func(T) bool

type Signal[T comparable] struct {
	mu *sync.Mutex
	t  *T

	inner []listener[T]
	outer chan T
}

func (h *Signal[T]) Actual() *T {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.t == nil {
		return nil
	}

	return h.copyValue()
}

func (h *Signal[T]) copyValue() *T {
	if h.t == nil {
		return nil
	}

	data, err := json.Marshal(*h.t)
	if err != nil {
		panic(err)
	}

	var ret T
	if err := json.Unmarshal(data, &ret); err != nil {
		panic(err)
	}

	return &ret
}

func (h *Signal[T]) Listen(filters ...ChangerFilter[T]) (<-chan T, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newChannel := make(chan T, 1)
	index := len(h.inner)
	h.inner = append(h.inner, listener[T]{
		channel: newChannel,
		filters: filters,
	})
	if h.t != nil {
		shouldSend := true
		newValue := *h.copyValue()
		for _, filter := range filters {
			if !filter(newValue) {
				shouldSend = false
				break
			}
		}

		if shouldSend {
			newChannel <- newValue
		}
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

func (h *Signal[T]) Signal(t T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.t = &t

	for _, inner := range h.inner {
		shouldSend := true
		for _, filter := range inner.filters {
			if !filter(t) {
				shouldSend = false
			}
		}

		if shouldSend {
			inner.channel <- t
		}
	}
}

func (h *Signal[T]) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, inner := range h.inner {
		close(inner.channel)
	}
}

func (h *Signal[T]) CountListeners() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.inner)
}

func NewSignal[T comparable](init *T) *Signal[T] {
	return &Signal[T]{
		outer: make(chan T),
		mu:    &sync.Mutex{},
		t:     init,
	}
}
