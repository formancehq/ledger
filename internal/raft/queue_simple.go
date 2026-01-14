package raft

type SimpleQueue[T any] struct {
	ch       chan T
	capacity int
}

func (q *SimpleQueue[T]) Push(msg T) bool {
	select {
	case q.ch <- msg:
		return true
	default:
		return false
	}
}

func (q *SimpleQueue[T]) Recv() <-chan T {
	return q.ch
}

func (q *SimpleQueue[T]) Close() {
	close(q.ch)
}

func (q *SimpleQueue[T]) Capacity() int {
	return q.capacity
}

func NewSimpleQueue[T any](capacity int) *SimpleQueue[T] {
	return &SimpleQueue[T]{
		ch:       make(chan T, capacity),
		capacity: capacity,
	}
}
