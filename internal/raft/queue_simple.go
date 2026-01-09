package raft

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
)

type SimpleQueue[T any] struct {
	in  chan T
	out chan T
	capacity int
}

func (q *SimpleQueue[T]) Push(msg T) bool {
	select {
	case q.in <- msg:
		return true
	default:
		return false
	}
}

func (q *SimpleQueue[T]) Recv() <-chan T {
	return q.out
}

func (q *SimpleQueue[T]) Close() {
	close(q.in)
}

func (q *SimpleQueue[T]) Capacity() int {
	return q.capacity
}

func NewSimpleQueue[T any](logger logging.Logger, capacity int) *SimpleQueue[T] {
	ret := &SimpleQueue[T]{
		out: make(chan T),
		in:  make(chan T, capacity),
		capacity: capacity,
	}

	otlplogs.Go(func() {
		for msg := range ret.in {
			ret.out <- msg
		}
	}, logger)

	return ret
}
