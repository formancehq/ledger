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

// Wait blocks until the future is resolved or the context is cancelled.
// If the context is cancelled before the future is resolved, the context
// error is returned. A nil context panics: every caller must surface
// cancellation, so accepting nil would silently re-introduce the
// uncancellable wait this API was changed to eliminate.
func (f *Future[T]) Wait(ctx context.Context) (T, error) {
	if ctx == nil {
		panic("futures: Wait called with nil context")
	}

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
	//
	// The broadcast MUST happen under f.mu, otherwise a lost-wakeup race exists:
	// the waiter could observe ctx.Err() == nil, the canceller could broadcast
	// before the waiter parks in cond.Wait(), and the waiter would then park
	// forever. Holding the lock during broadcast serializes against the waiter's
	// ctx.Err() check + Wait() pair.
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			f.mu.Lock()
			f.cond.Broadcast()
			f.mu.Unlock()
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
