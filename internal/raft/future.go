package raft

import "context"

// future represents a future for an applied entry
type future struct {
	ch     chan struct{}
	result any
	done   bool
	err    error
}

func (f *future) Result() any {
	return f.result
}

func (f *future) Done() chan struct{} {
	return f.ch
}

func (f *future) Resolve(result any, err error) {
	f.done = true
	f.result = result
	f.err = err
	close(f.ch)
}

func (f *future) wait(ctx context.Context) (any, error) {
	select {
	case <-f.Done():
		if f.err != nil {
			return nil, f.err
		}
		return f.Result(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newFuture() future {
	return future{
		ch: make(chan struct{}),
	}
}