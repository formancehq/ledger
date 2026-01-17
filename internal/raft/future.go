package raft

// future represents a future for an applied entry
type future struct {
	ch     chan error
	result any
	done   bool
	err    error
}

func (f *future) Err() <-chan error {
	return f.ch
}

func (f *future) Result() any {
	return f.result
}

func (f *future) Done() bool {
	return f.done
}

func (f *future) Resolve(result any, err error) {
	f.done = true
	f.result = result
	f.err = err
	// Send error (or nil) to channel
	select {
	case f.ch <- err:
	default:
		// Channel already closed or error already sent
	}
}

func newFuture() future {
	return future{
		ch: make(chan error, 1),
	}
}