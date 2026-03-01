package signal

// Notifications holds the signals shared between the FSM and a Manager
// (events or mirror). It is created independently (no dependency on Node or
// Manager) to break the circular dependency in the fx graph.
type Notifications struct {
	LogCommitted  Signal
	ConfigChanged Signal
}

// NewNotifications creates a new Notifications with buffered(1) signals.
func NewNotifications() *Notifications {
	return &Notifications{
		LogCommitted:  New(),
		ConfigChanged: New(),
	}
}

// NotifyLogsCommitted signals that new logs have been committed.
func (n *Notifications) NotifyLogsCommitted() {
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
