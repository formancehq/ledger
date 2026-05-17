package signal

import "sync/atomic"

// Notifications holds the signals shared between the FSM and a Manager
// (events or mirror). It is created independently (no dependency on Node or
// Manager) to break the circular dependency in the fx graph.
type Notifications struct {
	LogCommitted  Signal
	ConfigChanged Signal

	// LastSequence caches the most recent log sequence written by the FSM.
	// Updated atomically before LogCommitted fires, so readers can cheaply
	// obtain the latest Pebble sequence without opening an iterator.
	LastSequence atomic.Uint64
}

// NewNotifications creates a new Notifications with buffered(1) signals.
func NewNotifications() *Notifications {
	return &Notifications{
		LogCommitted:  New(),
		ConfigChanged: New(),
	}
}

// NotifyLogsCommitted stores the latest log sequence and signals that new logs
// have been committed.
func (n *Notifications) NotifyLogsCommitted(lastSeq uint64) {
	n.LastSequence.Store(lastSeq)
	n.LogCommitted.Notify()
}

// NotifyConfigChanged signals that the configuration has changed.
func (n *Notifications) NotifyConfigChanged() {
	n.ConfigChanged.Notify()
}

// RunNotificationLoop runs a select loop that listens for log-committed and
// config-changed signals. It calls onLogCommitted and onConfigChanged
// respectively, and returns when stop is closed.
func RunNotificationLoop(stop <-chan struct{}, notifications *Notifications, onLogCommitted, onConfigChanged func()) {
	for {
		select {
		case <-notifications.LogCommitted.C():
			onLogCommitted()
		case <-notifications.ConfigChanged.C():
			onConfigChanged()
		case <-stop:
			return
		}
	}
}

// FanOut dispatches NotifyLogsCommitted and NotifyConfigChanged to multiple
// Notifications targets. It decouples the FSM (single emitter) from the
// consumers (events Manager, mirror Manager, index Builder).
type FanOut struct {
	targets []*Notifications
}

// NewFanOut creates a FanOut that dispatches to the given targets.
func NewFanOut(targets ...*Notifications) *FanOut {
	return &FanOut{targets: targets}
}

// NotifyLogsCommitted stores the latest sequence and signals all targets.
func (f *FanOut) NotifyLogsCommitted(lastSeq uint64) {
	for _, t := range f.targets {
		t.NotifyLogsCommitted(lastSeq)
	}
}

// NotifyConfigChanged signals all targets that configuration has changed.
func (f *FanOut) NotifyConfigChanged() {
	for _, t := range f.targets {
		t.NotifyConfigChanged()
	}
}
