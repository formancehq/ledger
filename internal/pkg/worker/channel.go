package worker

import (
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// Channel is a buffered channel for dispatching background work items from the
// FSM apply path or recovery paths to background workers.
//
// It provides two send modes:
//   - TrySend: non-blocking, for the FSM hot path where blocking is forbidden.
//     Logs when a send is dropped so the condition is observable.
//   - Send: blocking with a stop channel, for recovery and reconciliation paths
//     where waiting for the worker to drain is acceptable.
type Channel[T any] struct {
	ch     chan T
	logger logging.Logger
	name   string
}

// NewChannel creates a Channel with the given buffer size.
// name is used in log messages when a TrySend is dropped.
func NewChannel[T any](logger logging.Logger, name string, bufferSize int) *Channel[T] {
	return &Channel[T]{
		ch:     make(chan T, bufferSize),
		logger: logger,
		name:   name,
	}
}

// TrySend attempts a non-blocking send. Returns true if the value was sent.
// On drop, logs an error with the provided detail message.
// Use from the FSM apply path where blocking is forbidden.
func (wc *Channel[T]) TrySend(value T, detail string) bool {
	select {
	case wc.ch <- value:
		return true
	default:
		wc.logger.Errorf("Dropped %s: %s (channel full, reconciliation will re-dispatch)", wc.name, detail)

		return false
	}
}

// Send blocks until the value is sent or stop is closed.
// Returns true if sent, false if stop was closed.
// Use from recovery and reconciliation paths where blocking is safe.
func (wc *Channel[T]) Send(value T, stop <-chan struct{}) bool {
	select {
	case wc.ch <- value:
		return true
	case <-stop:
		return false
	}
}

// Receive returns the receive-only end of the channel for DrainChannel.
func (wc *Channel[T]) Receive() <-chan T {
	return wc.ch
}
