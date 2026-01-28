package raft

import (
	"sync"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// future represents a future for an applied entry
type future struct {
	mu   sync.Mutex
	cond *sync.Cond
	done bool
	err  error
	logs []*commonpb.Log
}

func (f *future) Logs() []*commonpb.Log {
	return f.logs
}

func (f *future) Resolve(logs []*commonpb.Log, err error) {
	f.mu.Lock()
	f.done = true
	f.err = err
	f.logs = logs
	f.cond.Signal()
	f.mu.Unlock()
}

func (f *future) wait() ([]*commonpb.Log, error) {
	f.mu.Lock()
	for !f.done {
		f.cond.Wait()
	}
	err := f.err
	logs := f.logs
	f.mu.Unlock()

	return logs, err
}

func newFuture() *future {
	ret := &future{}
	ret.cond = sync.NewCond(&ret.mu)
	return ret
}
