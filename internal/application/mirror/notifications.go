package mirror

import "github.com/formancehq/ledger-v3-poc/internal/pkg/signal"

// Notifications holds the signals shared between the FSM and the mirror Manager.
// It is created independently to break circular dependencies in the fx graph.
type Notifications struct {
	LogCommitted  signal.Signal
	ConfigChanged signal.Signal
}

// NewNotifications creates a new Notifications with buffered(1) signals.
func NewNotifications() *Notifications {
	return &Notifications{
		LogCommitted:  signal.New(),
		ConfigChanged: signal.New(),
	}
}

// NotifyLogsCommitted signals that new logs have been committed.
func (n *Notifications) NotifyLogsCommitted() {
	n.LogCommitted.Notify()
}

// NotifyConfigChanged signals that a mirror ledger was created or promoted.
func (n *Notifications) NotifyConfigChanged() {
	n.ConfigChanged.Notify()
}
