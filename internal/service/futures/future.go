package futures

import (
	"sync"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// Future represents a future for an applied entry
type Future struct {
	mu   sync.Mutex
	cond *sync.Cond
	done bool
	err  error
	logs []*commonpb.Log
}

func (f *Future) Logs() []*commonpb.Log {
	return f.logs
}

func (f *Future) Resolve(logs []*commonpb.Log, err error) {
	f.mu.Lock()
	f.done = true
	f.err = err
	f.logs = logs
	f.cond.Signal()
	f.mu.Unlock()
}

func (f *Future) Wait() ([]*commonpb.Log, error) {
	f.mu.Lock()
	for !f.done {
		f.cond.Wait()
	}
	err := f.err
	logs := f.logs
	f.mu.Unlock()

	return logs, err
}

func NewFuture() *Future {
	ret := &Future{}
	ret.cond = sync.NewCond(&ret.mu)
	return ret
}
