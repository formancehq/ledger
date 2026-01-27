package raft

import "fmt"

type panickedError struct {
	stack []byte
	e     any
}

func (e *panickedError) Error() string {
	return fmt.Sprintf("panic: %v\n%s", e.e, e.stack)
}

func (e *panickedError) Unwrap() error {
	switch e := e.e.(type) {
	case error:
		return e
	default:
		return nil
	}
}

func newPanickedError(e any, stack []byte) *panickedError {
	return &panickedError{
		e: e,
		stack: stack,
	}
}
