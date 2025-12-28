package raft


type SimpleQueue[T any] struct {
	in  chan T
	out chan T
}

func (q *SimpleQueue[T]) Send(msg T) bool {
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

func NewSimpleQueue[T any](options ...SimpleQueueOption[T]) *SimpleQueue[T] {
	ret := &SimpleQueue[T]{
		out: make(chan T),
	}
	for _, option := range append(defaultSimpleQueueOptions[T](), options...) {
		option(ret)
	}
	go func() {
		for msg := range ret.in {
			ret.out <- msg
		}
	}()
	return ret
}

type SimpleQueueOption[T any] func(queue *SimpleQueue[T])

func WithSimpleQueueSize[T any](size int) SimpleQueueOption[T] {
	return func(ch *SimpleQueue[T]) {
		ch.in = make(chan T, size)
	}
}

func defaultSimpleQueueOptions[T any]() []SimpleQueueOption[T] {
	return []SimpleQueueOption[T]{
		WithSimpleQueueSize[T](100),
	}
}
