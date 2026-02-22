package futures

import "sync"

// Future is a generic, goroutine-safe one-shot value container.
// It is resolved exactly once by the producer and can be awaited by the consumer.
type Future[T any] struct {
	mu    sync.Mutex
	cond  *sync.Cond
	done  bool
	err   error
	value T
}

func (f *Future[T]) Resolve(value T, err error) {
	f.mu.Lock()
	f.done = true
	f.err = err
	f.value = value
	f.cond.Broadcast()
	f.mu.Unlock()
}

func (f *Future[T]) Wait() (T, error) {
	f.mu.Lock()
	for !f.done {
		f.cond.Wait()
	}
	value := f.value
	err := f.err
	f.mu.Unlock()

	return value, err
}

func New[T any]() *Future[T] {
	ret := &Future[T]{}
	ret.cond = sync.NewCond(&ret.mu)
	return ret
}
