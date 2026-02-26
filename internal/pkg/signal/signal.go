package signal

// Signal is a non-blocking notification mechanism backed by a buffered(1) channel.
// Multiple Notify() calls coalesce into a single pending notification.
type Signal struct {
	ch chan struct{}
}

// New creates a new Signal with a buffered(1) channel.
func New() Signal {
	return Signal{ch: make(chan struct{}, 1)}
}

// Notify sends a non-blocking signal. If a signal is already pending, this is a no-op.
func (s Signal) Notify() {
	select {
	case s.ch <- struct{}{}:
	default:
	}
}

// C returns the receive-only channel for use in select statements.
func (s Signal) C() <-chan struct{} {
	return s.ch
}
