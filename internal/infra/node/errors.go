package node

import "fmt"

// RemoveNodeCommittedError reports that a ConfChangeRemoveNode is already
// committed in Raft, but the caller stopped waiting before the corresponding
// FSM batch (including the removed-member tombstone) became durable.
//
// Retrying the same removal is safe: the target is already absent from Raft.
// Callers should verify the membership postcondition instead of treating this
// as evidence that the configuration change failed.
type RemoveNodeCommittedError struct {
	NodeID       uint64
	AppliedIndex uint64
	Cause        error
}

func (e *RemoveNodeCommittedError) Error() string {
	return fmt.Sprintf(
		"node %d removal committed at raft index %d; removed-member tombstone application is still pending",
		e.NodeID,
		e.AppliedIndex,
	)
}

func (e *RemoveNodeCommittedError) Unwrap() error {
	return e.Cause
}

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
		e:     e,
		stack: stack,
	}
}
