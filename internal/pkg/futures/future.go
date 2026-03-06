package futures

import (
	"context"
	"sync"
)

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

// WaitContext waits for the future to be resolved or the context to be cancelled.
// If the context is cancelled before the future is resolved, the context error is returned.
func (f *Future[T]) WaitContext(ctx context.Context) (T, error) {
	// Fast path: already done or context already cancelled.
	f.mu.Lock()
	if f.done {
		value, err := f.value, f.err
		f.mu.Unlock()

		return value, err
	}
	f.mu.Unlock()

	if err := ctx.Err(); err != nil {
		var zero T

		return zero, err
	}

	// Spawn a goroutine that broadcasts on the cond when the context is cancelled,
	// so any cond.Wait() call below wakes up and can check ctx.Err().
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			f.cond.Broadcast()
		case <-done:
		}
	}()

	f.mu.Lock()
	for !f.done {
		if ctx.Err() != nil {
			f.mu.Unlock()

			var zero T

			return zero, ctx.Err()
		}

		f.cond.Wait()
	}

	value, err := f.value, f.err
	f.mu.Unlock()

	return value, err
}

func New[T any]() *Future[T] {
	ret := &Future[T]{}
	ret.cond = sync.NewCond(&ret.mu)

	return ret
}
