package events

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const sinkStartupRetryDelay = time.Second
const sinkStartupErrorPrefix = "sink startup: "

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

	mu                  sync.Mutex
	emitters            map[string]*managedSink
	retries             map[string]struct{}
	resourcesGeneration uint64

	leadershipMu         sync.Mutex
	leadershipGeneration uint64
	isLeader             bool
	stopped              bool
	leaderContext        context.Context
	leaderCancel         context.CancelFunc

	w worker.Worker
	// readSinkCursor is an optional startup read hook used by tests.
	readSinkCursor func(dal.PebbleGetter, string) (uint64, error)
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
	m.leadershipMu.Lock()
	if m.stopped {
		m.leadershipMu.Unlock()

		return
	}

	m.leadershipGeneration++
	m.isLeader = false
	m.stopped = true
	if m.leaderCancel != nil {
		m.leaderCancel()
	}
	m.leadershipMu.Unlock()

	m.w.Stop()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.teardown()
}

// OnLeadershipChange records the latest leadership generation and wakes the
// lifecycle-owned reconciliation loop. It deliberately does not reconcile on
// the caller: bootstrap invokes it synchronously from the Raft observer so
// transitions are recorded in order without blocking the Raft processing loop
// on Pebble reads or worker startup.
func (m *Manager) OnLeadershipChange(isLeader bool) {
	m.leadershipMu.Lock()
	if m.stopped {
		m.leadershipMu.Unlock()

		return
	}

	m.leadershipGeneration++
	m.isLeader = isLeader
	if m.leaderCancel != nil {
		m.leaderCancel()
	}
	m.leaderContext, m.leaderCancel = context.WithCancel(context.Background())
	m.leadershipMu.Unlock()

	m.notifications.NotifyConfigChanged()
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

			m.reconcile()
		},
	)
}

func (m *Manager) leadershipSnapshot() (uint64, bool, bool) {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()

	return m.leadershipGeneration, m.isLeader, m.stopped
}

func (m *Manager) isCurrentLeader(generation uint64) bool {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()

	return !m.stopped && m.isLeader && m.leadershipGeneration == generation
}

func (m *Manager) currentLeaderContext(generation uint64) (context.Context, bool) {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()
	if m.stopped || !m.isLeader || m.leadershipGeneration != generation {
		return nil, false
	}

	return m.leaderContext, true
}

func (m *Manager) isCurrentGeneration(generation uint64) bool {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()

	return m.leadershipGeneration == generation
}

// reconcile reads the current per-sink configurations from the store and
// starts, stops, or restarts emitters/sinks as needed. Only sinks that
// were added, removed, or changed are affected — unchanged sinks keep running.
// Must be called under lock.
func (m *Manager) reconcile() {
	generation, isLeader, stopped := m.leadershipSnapshot()
	m.reconcileGeneration(generation, isLeader, stopped)
}

// reconcileGeneration applies one captured leadership generation. A transition
// can arrive after the loop wakes but before it acquires m.mu, so reject a
// superseded generation before either teardown or startup mutates ownership.
// Must be called under lock.
func (m *Manager) reconcileGeneration(generation uint64, isLeader, stopped bool) {
	if !m.isCurrentGeneration(generation) {
		return
	}
	if m.resourcesGeneration != generation {
		m.teardown()
		m.resourcesGeneration = generation
	}

	if stopped || !isLeader {
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

	if !m.isCurrentLeader(generation) {
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
		if !m.isCurrentLeader(generation) {
			return
		}

		sc, stillDesired := desired[name]
		if !stillDesired || !sc.EqualVT(ms.config) {
			m.stopSink(name, ms)
			delete(m.emitters, name)
		}
	}

	// Start sinks that are new or were just removed due to config change
	for name, sc := range desired {
		if !m.isCurrentLeader(generation) {
			return
		}

		if _, exists := m.emitters[name]; exists {
			continue // already running with same config
		}

		if ms := m.startSink(sc, generation); ms != nil {
			if !m.isCurrentLeader(generation) {
				m.stopSink(name, ms)

				return
			}

			m.emitters[name] = ms
			delete(m.retries, name)
		}
	}

	m.logger.Infof("Events emitters reconciled (active=%d)", len(m.emitters))
}

// startSink creates and starts an emitter+sink pair from a SinkConfig.
// Returns nil if the sink type is unsupported or creation fails.
func (m *Manager) startSink(sc *commonpb.SinkConfig, generation uint64) *managedSink {
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
		m.reportStartupError(sc.GetName(), err, generation)
		m.scheduleStartupRetry(sc.GetName())

		return nil
	}

	emitter := NewEmitter(m.store, sink, sc.GetName(), m.proposer, m.builder, m.logger, emitterCfg)
	if m.readSinkCursor != nil {
		emitter.readCursor = m.readSinkCursor
	}
	startupReady := make(chan struct{})
	emitter.startupReady = startupReady
	emitter.Start()
	if err := emitter.WaitStarted(context.Background()); err != nil {
		m.logger.Errorf("Failed to start sink %q: %v", sc.GetName(), err)
		emitter.Stop()

		if closeErr := sink.Close(); closeErr != nil {
			m.logger.Errorf("Failed to close sink %q after startup failure: %v", sc.GetName(), closeErr)
		}
		m.reportStartupError(sc.GetName(), err, generation)

		m.scheduleStartupRetry(sc.GetName())

		return nil
	}
	if err := m.clearStartupError(emitter, generation); err != nil {
		m.logger.Errorf("Failed to clear startup error for sink %q: %v", sc.GetName(), err)
		emitter.Stop()
		if closeErr := sink.Close(); closeErr != nil {
			m.logger.Errorf("Failed to close sink %q after status failure: %v", sc.GetName(), closeErr)
		}
		m.scheduleStartupRetry(sc.GetName())

		return nil
	}
	if !m.isCurrentLeader(generation) {
		emitter.Stop()
		if closeErr := sink.Close(); closeErr != nil {
			m.logger.Errorf("Failed to close sink %q after leadership loss: %v", sc.GetName(), closeErr)
		}

		return nil
	}
	close(startupReady)

	return &managedSink{emitter: emitter, sink: sink, config: sc}
}

func (m *Manager) readSinkStatus(name string) (*commonpb.SinkStatus, error) {
	handle, err := m.store.NewDirectReadHandle()
	if err != nil {
		return nil, err
	}
	defer func() { _ = handle.Close() }()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkStatus).PutString(name)
	status, err := dal.ReadProto[*commonpb.SinkStatus](handle, kb.Build())
	if err != nil {
		return nil, fmt.Errorf("reading sink status for %q: %w", name, err)
	}

	return status, nil
}

// Startup errors use the existing replicated technical status. The prefix
// distinguishes them from delivery errors when a new leader starts a sink.
func (m *Manager) reportStartupError(name string, startupErr error, generation uint64) {
	if !m.isCurrentLeader(generation) {
		return
	}
	message := sinkStartupErrorPrefix + startupErr.Error()
	status, err := m.readSinkStatus(name)
	if err != nil {
		m.logger.Errorf("Failed to read status for sink %q: %v", name, err)

		return
	}
	if status.GetError().GetMessage() == message {
		return
	}
	leaderContext, current := m.currentLeaderContext(generation)
	if !current {
		return
	}
	update := &raftcmdpb.EventsSinkUpdate{SinkName: name, Error: &commonpb.SinkError{
		Message: message, OccurredAt: commonpb.NewTimestamp(libtime.Now()),
	}}
	ctx, cancel := context.WithTimeout(leaderContext, deliveredCursorUpdateTimeout)
	defer cancel()
	if err := NewEmitter(m.store, nil, name, m.proposer, m.builder, m.logger, DefaultEmitterConfig()).proposeSinkUpdate(ctx, update); err != nil {
		m.logger.Errorf("Failed to report startup error for sink %q: %v", name, err)
	}
}

func (m *Manager) clearStartupError(emitter *Emitter, generation uint64) error {
	if !m.isCurrentLeader(generation) {
		return nil
	}
	status, err := m.readSinkStatus(emitter.sinkName)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(status.GetError().GetMessage(), sinkStartupErrorPrefix) {
		return nil
	}
	leaderContext, current := m.currentLeaderContext(generation)
	if !current {
		return nil
	}
	ctx, cancel := context.WithTimeout(leaderContext, deliveredCursorUpdateTimeout)
	defer cancel()

	return emitter.proposeSinkUpdate(ctx, &raftcmdpb.EventsSinkUpdate{
		SinkName: emitter.sinkName, ClearError: true,
	})
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
		_, isLeader, stopped := m.leadershipSnapshot()
		m.mu.Unlock()

		if isLeader && !stopped {
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
