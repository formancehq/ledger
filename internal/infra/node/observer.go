package node

import (
	"go.etcd.io/raft/v3/raftpb"
)

// ConfChangeEvent is emitted when a Raft configuration change is committed.
type ConfChangeEvent struct {
	NodeID     uint64
	ChangeType raftpb.ConfChangeType
	Context    []byte
}

// LeadershipChangeEvent is emitted when the node's leadership status changes.
// Emitted synchronously in the Raft processing loop, BEFORE the FSM catches up.
type LeadershipChangeEvent struct {
	IsLeader bool
}

// LeaderReadyEvent is emitted after a node becomes leader AND the FSM has
// caught up with all committed entries. At this point, the Pebble state is
// up to date and it is safe to read persisted config and propose updates.
type LeaderReadyEvent struct{}

// EventHandler is a callback invoked synchronously for each emitted event.
// Consumers use a type switch on the event to handle concrete types.
type EventHandler func(event any)

// Observer dispatches events synchronously to a registered handler.
// It is called inline in the Raft processing loop so that side-effects
// (e.g. adding a gRPC peer) are visible before the next message is sent.
type Observer struct {
	handler EventHandler
}

// NewObserver creates an Observer that forwards every event to handler.
func NewObserver(handler EventHandler) *Observer {
	return &Observer{handler: handler}
}

// NewNoOpObserver creates an Observer that silently discards all events.
func NewNoOpObserver() *Observer {
	return &Observer{handler: func(any) {}}
}

// Emit invokes the handler synchronously with the given event.
func (o *Observer) Emit(event any) {
	o.handler(event)
}
