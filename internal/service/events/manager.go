package events

import (
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

// managedSink holds an emitter and its sink for a named sink configuration.
type managedSink struct {
	emitter *Emitter
	sink    Sink
	config  *commonpb.SinkConfig
}

// Notifications holds the signals shared between the FSM and the event Manager.
// It is created independently (no dependency on Node or Manager) to break the
// circular dependency in the fx graph.
type Notifications struct {
	LogCommitted  Signal
	ConfigChanged Signal
}

// NewNotifications creates a new Notifications with buffered(1) signals.
func NewNotifications() *Notifications {
	return &Notifications{
		LogCommitted:  NewSignal(),
		ConfigChanged: NewSignal(),
	}
}

// NotifyLogsCommitted signals that new logs have been committed.
func (n *Notifications) NotifyLogsCommitted() {
	n.LogCommitted.Notify()
}

// NotifyConfigChanged signals that the events configuration has changed.
func (n *Notifications) NotifyConfigChanged() {
	n.ConfigChanged.Notify()
}

// Manager manages the lifecycle of event emitters and sinks based on
// the Raft-replicated events configuration. It creates one Emitter per
// named sink, each with its own cursor and error status.
type Manager struct {
	store         *data.Store
	proposer      Proposer
	logger        logging.Logger
	notifications *Notifications

	mu       sync.Mutex
	isLeader bool
	emitters map[string]*managedSink

	stopCh  chan struct{}
	stopped chan struct{}
}

// NewManager creates a new event Manager.
func NewManager(store *data.Store, proposer Proposer, logger logging.Logger, notifications *Notifications) *Manager {
	return &Manager{
		store:         store,
		proposer:      proposer,
		logger:        logger.WithFields(map[string]any{"cmp": "event-manager"}),
		notifications: notifications,
		emitters:      make(map[string]*managedSink),
	}
}

// Start begins the background goroutine that listens for log notifications
// and config changes.
func (m *Manager) Start() {
	m.stopCh = make(chan struct{})
	m.stopped = make(chan struct{})
	go m.run()
}

// Stop gracefully stops the Manager and tears down any active emitters/sinks.
func (m *Manager) Stop() {
	close(m.stopCh)
	<-m.stopped

	m.mu.Lock()
	defer m.mu.Unlock()
	m.teardown()
}

// OnLeadershipChange is called when the node's leadership status changes.
func (m *Manager) OnLeadershipChange(isLeader bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.isLeader = isLeader
	m.reconcile()
}

func (m *Manager) run() {
	defer close(m.stopped)

	for {
		select {
		case <-m.notifications.LogCommitted.C():
			m.mu.Lock()
			// Forward notification to all active emitters
			for _, ms := range m.emitters {
				ms.emitter.Notify()
			}
			m.mu.Unlock()
		case <-m.notifications.ConfigChanged.C():
			m.mu.Lock()
			if m.isLeader {
				m.reconcile()
			}
			m.mu.Unlock()
		case <-m.stopCh:
			return
		}
	}
}

// reconcile reads the current per-sink configurations from the store and
// starts, stops, or restarts emitters/sinks as needed. Only sinks that
// were added, removed, or changed are affected — unchanged sinks keep running.
// Must be called under lock.
func (m *Manager) reconcile() {
	if !m.isLeader {
		m.teardown()
		return
	}

	sinkCfgs, err := m.store.LoadAllSinkConfigs()
	if err != nil {
		m.logger.Errorf("Failed to load sink configs: %v", err)
		return
	}

	// Build desired state as a map keyed by sink name
	desired := make(map[string]*commonpb.SinkConfig, len(sinkCfgs))
	for _, sc := range sinkCfgs {
		if sc.Name == "" {
			m.logger.Errorf("Sink config has empty name, skipping")
			continue
		}
		desired[sc.Name] = sc
	}

	// Remove sinks that no longer exist or whose config changed
	for name, ms := range m.emitters {
		sc, stillDesired := desired[name]
		if !stillDesired || !sc.EqualVT(ms.config) {
			m.stopSink(name, ms)
			delete(m.emitters, name)
		}
	}

	// Start sinks that are new or were just removed due to config change
	for name, sc := range desired {
		if _, exists := m.emitters[name]; exists {
			continue // already running with same config
		}
		if ms := m.startSink(sc); ms != nil {
			m.emitters[name] = ms
		}
	}

	m.logger.Infof("Events emitters reconciled (active=%d)", len(m.emitters))
}

// startSink creates and starts an emitter+sink pair from a SinkConfig.
// Returns nil if the sink type is unsupported or creation fails.
func (m *Manager) startSink(sc *commonpb.SinkConfig) *managedSink {
	emitterCfg := DefaultEmitterConfig()
	if sc.Format != "" {
		emitterCfg.Format = Format(sc.Format)
	}
	if sc.BatchSize > 0 {
		emitterCfg.BatchSize = int(sc.BatchSize)
	}
	if sc.BatchDelayMs > 0 {
		emitterCfg.BatchDelay = time.Duration(sc.BatchDelayMs) * time.Millisecond
	}

	sink, err := m.createSink(sc)
	if err != nil {
		m.logger.Errorf("Failed to create sink %q: %v", sc.Name, err)
		return nil
	}

	emitter := NewEmitter(m.store, sink, sc.Name, m.proposer, m.logger, emitterCfg)
	emitter.Start()
	return &managedSink{emitter: emitter, sink: sink, config: sc}
}

// stopSink stops an emitter and closes its sink.
func (m *Manager) stopSink(name string, ms *managedSink) {
	ms.emitter.Stop()
	if err := ms.sink.Close(); err != nil {
		m.logger.Errorf("Failed to close sink %q: %v", name, err)
	}
}

// teardown stops all emitters and closes all sinks. Must be called under lock.
func (m *Manager) teardown() {
	for name, ms := range m.emitters {
		m.stopSink(name, ms)
	}
	m.emitters = make(map[string]*managedSink)
}

// createSink creates a single Sink from a SinkConfig entry.
func (m *Manager) createSink(sc *commonpb.SinkConfig) (Sink, error) {
	format := Format(sc.Format)
	if format == "" {
		format = FormatJSON
	}

	switch s := sc.GetType().(type) {
	case *commonpb.SinkConfig_Nats:
		return NewNATSSink(NATSSinkConfig{
			URL:    s.Nats.Url,
			Topic:  s.Nats.Topic,
			Format: format,
		})
	default:
		return nil, fmt.Errorf("unsupported events sink type: %T", s)
	}
}
