package events

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const sinkStartupRetryDelay = time.Second

// managedSink holds an emitter and its sink for a named sink configuration.
type managedSink struct {
	emitter *Emitter
	sink    Sink
	config  *commonpb.SinkConfig
}

// Manager manages the lifecycle of event emitters and sinks based on
// the Raft-replicated events configuration. It creates one Emitter per
// named sink, each with its own cursor and error status.
type Manager struct {
	store          *dal.Store
	sinkConfigAttr *attributes.Attribute[*commonpb.SinkConfig]
	proposer       Proposer
	builder        *plan.Builder
	logger         logging.Logger
	notifications  *signal.Notifications

	mu       sync.Mutex
	isLeader bool
	emitters map[string]*managedSink
	retries  map[string]struct{}

	w worker.Worker
}

// NewManager creates a new event Manager.
func NewManager(store *dal.Store, attrs *attributes.Attributes, proposer Proposer, builder *plan.Builder, logger logging.Logger, notifications *signal.Notifications) *Manager {
	return &Manager{
		store:          store,
		sinkConfigAttr: attrs.SinkConfig,
		proposer:       proposer,
		builder:        builder,
		logger:         logger.WithFields(map[string]any{"cmp": "event-manager"}),
		notifications:  notifications,
		emitters:       make(map[string]*managedSink),
		retries:        make(map[string]struct{}),
	}
}

// Start begins the background goroutine that listens for log notifications
// and config changes.
func (m *Manager) Start() {
	m.w = worker.New()
	m.w.Run(m.loop)
}

// Stop gracefully stops the Manager and tears down any active emitters/sinks.
func (m *Manager) Stop() {
	m.w.Stop()

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

func (m *Manager) loop(stop <-chan struct{}) {
	signal.RunNotificationLoop(stop, m.notifications,
		func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			// Forward notification to all active emitters
			for _, ms := range m.emitters {
				ms.emitter.Notify()
			}
		},
		func() {
			m.mu.Lock()
			defer m.mu.Unlock()

			if m.isLeader {
				m.reconcile()
			}
		},
	)
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

	cfgHandle, handleErr := m.store.NewDirectReadHandle()
	if handleErr != nil {
		m.logger.Errorf("Failed to create read handle: %v", handleErr)

		return
	}

	sinkCfgs, err := query.ReadAllSinkConfigs(m.sinkConfigAttr, cfgHandle)
	_ = cfgHandle.Close()

	if err != nil {
		m.logger.Errorf("Failed to load sink configs: %v", err)

		return
	}

	// Build desired state as a map keyed by sink name
	desired := make(map[string]*commonpb.SinkConfig, len(sinkCfgs))
	for _, sc := range sinkCfgs {
		if sc.GetName() == "" {
			m.logger.Errorf("Sink config has empty name, skipping")

			continue
		}

		desired[sc.GetName()] = sc
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
			delete(m.retries, name)
		}
	}

	m.logger.Infof("Events emitters reconciled (active=%d)", len(m.emitters))
}

// startSink creates and starts an emitter+sink pair from a SinkConfig.
// Returns nil if the sink type is unsupported or creation fails.
func (m *Manager) startSink(sc *commonpb.SinkConfig) *managedSink {
	emitterCfg := DefaultEmitterConfig()
	if sc.GetFormat() != "" {
		emitterCfg.Format = Format(sc.GetFormat())
	}

	if sc.GetBatchSize() > 0 {
		emitterCfg.BatchSize = int(sc.GetBatchSize())
	}

	if sc.GetBatchDelayMs() > 0 {
		emitterCfg.BatchDelay = time.Duration(sc.GetBatchDelayMs()) * time.Millisecond
	}

	if len(sc.GetEventTypes()) > 0 {
		emitterCfg.EventTypes = make(map[commonpb.EventType]struct{}, len(sc.GetEventTypes()))
		for _, et := range sc.GetEventTypes() {
			emitterCfg.EventTypes[et] = struct{}{}
		}
	}

	sink, err := m.createSink(sc)
	if err != nil {
		m.logger.Errorf("Failed to create sink %q: %v", sc.GetName(), err)

		return nil
	}

	emitter := NewEmitter(m.store, sink, sc.GetName(), m.proposer, m.builder, m.logger, emitterCfg)
	emitter.Start()
	if err := emitter.WaitStarted(context.Background()); err != nil {
		m.logger.Errorf("Failed to start sink %q: %v", sc.GetName(), err)
		emitter.Stop()

		if closeErr := sink.Close(); closeErr != nil {
			m.logger.Errorf("Failed to close sink %q after startup failure: %v", sc.GetName(), closeErr)
		}

		m.scheduleStartupRetry(sc.GetName())

		return nil
	}

	return &managedSink{emitter: emitter, sink: sink, config: sc}
}

// scheduleStartupRetry triggers a future reconcile for transient startup
// failures without spinning the notification loop. Must be called under lock.
func (m *Manager) scheduleStartupRetry(name string) {
	if _, exists := m.retries[name]; exists {
		return
	}

	m.retries[name] = struct{}{}
	stopCh := m.w.StopCh()

	go func() {
		timer := time.NewTimer(sinkStartupRetryDelay)
		defer timer.Stop()

		select {
		case <-stopCh:
			return
		case <-timer.C:
		}

		m.mu.Lock()
		if _, pending := m.retries[name]; !pending {
			m.mu.Unlock()

			return
		}

		delete(m.retries, name)
		isLeader := m.isLeader
		m.mu.Unlock()

		if isLeader {
			m.notifications.NotifyConfigChanged()
		}
	}()
}

// stopSink stops an emitter and closes its sink.
func (m *Manager) stopSink(name string, ms *managedSink) {
	ms.emitter.Stop()

	err := ms.sink.Close()
	if err != nil {
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
// HTTP sinks are always available. Other sink types (Kafka, NATS, ClickHouse)
// are only available when compiled with their respective build tags.
func (m *Manager) createSink(sc *commonpb.SinkConfig) (Sink, error) {
	format := Format(sc.GetFormat())
	if format == "" {
		format = FormatJSON
	}

	// HTTP sink is always available (no heavy dependencies).
	if s, ok := sc.GetType().(*commonpb.SinkConfig_Http); ok {
		return NewHTTPSink(HTTPSinkConfig{
			Endpoint: s.Http.GetEndpoint(),
			Secret:   s.Http.GetSecret(),
			Format:   format,
		})
	}

	// Look up optional sinks in the registry.
	typeName := sinkTypeName(sc)

	factory, ok := sinkFactories[typeName]
	if !ok {
		return nil, fmt.Errorf("unsupported events sink type: %s (not compiled in this build)", typeName)
	}

	return factory(sc, format)
}
