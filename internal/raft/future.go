package raft

import (
	"sync"
)

// future represents a future for an applied entry
type future struct {
	mu     sync.Mutex
	cond   *sync.Cond
	done   bool
	err    error
	result any
}

func (f *future) Result() any {
	return f.result
}

func (f *future) Resolve(result any, err error) {
	f.mu.Lock()
	f.done = true
	f.err = err
	f.result = result
	f.cond.Signal()
	f.mu.Unlock()
}

func (f *future) wait() (any, error) {
	f.mu.Lock()
	for !f.done {
		f.cond.Wait()
	}
	err := f.err
	result := f.result
	f.mu.Unlock()

	return result, err
}

func newFuture() *future {
	ret := &future{}
	ret.cond = sync.NewCond(&ret.mu)
	return ret
}
