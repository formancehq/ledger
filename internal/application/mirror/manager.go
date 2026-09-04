package mirror

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/adapter/v2/celrewrite"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Proposer submits a Raft proposal and returns a future for the apply result.
type Proposer interface {
	Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error)
}

// Manager manages the lifecycle of mirror workers based on the Raft-replicated
// ledger configuration. It creates one Worker per mirror ledger and only runs
// workers on the leader node.
type Manager struct {
	store         *dal.Store
	proposer      Proposer
	builder       *plan.Builder
	logger        logging.Logger
	notifications *signal.Notifications
	meterProvider metric.MeterProvider
	maxBatchSize  int

	mu                  sync.Mutex
	workers             map[string]*Worker
	resourcesGeneration uint64

	leadershipMu         sync.Mutex
	leadershipGeneration uint64
	isLeader             bool
	stopped              bool

	w worker.Worker
}

// NewManager creates a new mirror Manager.
func NewManager(store *dal.Store, proposer Proposer, builder *plan.Builder, logger logging.Logger, notifications *signal.Notifications, meterProvider metric.MeterProvider, maxBatchSize int) *Manager {
	return &Manager{
		store:         store,
		proposer:      proposer,
		builder:       builder,
		logger:        logger.WithFields(map[string]any{"cmp": "mirror-manager"}),
		notifications: notifications,
		meterProvider: meterProvider,
		maxBatchSize:  maxBatchSize,
		workers:       make(map[string]*Worker),
	}
}

// Start begins the background goroutine that listens for log notifications
// and config changes.
func (m *Manager) Start() {
	m.w = worker.New()
	m.w.Run(m.loop)
}

// Stop gracefully stops the Manager and tears down any active workers.
func (m *Manager) Stop() {
	m.leadershipMu.Lock()
	if m.stopped {
		m.leadershipMu.Unlock()

		return
	}

	m.leadershipGeneration++
	m.isLeader = false
	m.stopped = true
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
	m.leadershipMu.Unlock()

	m.notifications.NotifyConfigChanged()
}

func (m *Manager) loop(stop <-chan struct{}) {
	signal.RunNotificationLoop(stop, m.notifications,
		func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			// Forward notification to all active workers
			for _, w := range m.workers {
				w.Notify()
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

func (m *Manager) isCurrentGeneration(generation uint64) bool {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()

	return m.leadershipGeneration == generation
}

// reconcile reads the current mirror ledger configurations from the store and
// starts or stops workers as needed. Must be called under lock.
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

	mirrorHandle, handleErr := m.store.NewDirectReadHandle()
	if handleErr != nil {
		m.logger.Errorf("Failed to create read handle: %v", handleErr)

		return
	}

	mirrorLedgers, err := query.ReadMirrorLedgers(context.Background(), mirrorHandle)
	_ = mirrorHandle.Close()

	if err != nil {
		m.logger.Errorf("Failed to load mirror ledgers: %v", err)

		return
	}

	if !m.isCurrentLeader(generation) {
		return
	}

	// Build desired state as a set of ledger names
	desired := make(map[string]*commonpb.LedgerInfo, len(mirrorLedgers))
	for _, info := range mirrorLedgers {
		desired[info.GetName()] = info
	}

	// Remove workers for ledgers that are no longer mirrors
	for name, w := range m.workers {
		if !m.isCurrentLeader(generation) {
			return
		}

		if _, stillDesired := desired[name]; !stillDesired {
			w.Stop()
			delete(m.workers, name)
		}
	}

	// Start workers for new mirror ledgers
	for name, info := range desired {
		if !m.isCurrentLeader(generation) {
			return
		}

		if _, exists := m.workers[name]; exists {
			continue // already running
		}

		if info.GetMirrorSource() == nil {
			m.logger.WithFields(map[string]any{"ledger": name}).Errorf("Mirror ledger has no source config")

			continue
		}

		source, err := createSource(info.GetMirrorSource())
		if err != nil {
			m.logger.WithFields(map[string]any{"ledger": name}).Errorf("Failed to create mirror source: %v", err)

			continue
		}

		rewriter, err := celrewrite.NewRewriter(info.GetMirrorSource().GetRewriteRules())
		if err != nil {
			// Rules are validated at admission time, so a compile failure here
			// indicates corrupted config; skip the worker rather than crash.
			m.logger.WithFields(map[string]any{"ledger": name}).Errorf("Failed to build mirror rewriter: %v", err)

			continue
		}

		batchSize := int(info.GetMirrorSource().GetBatchSize())
		if m.maxBatchSize > 0 && (batchSize <= 0 || batchSize > m.maxBatchSize) {
			batchSize = m.maxBatchSize
		}

		w := NewWorker(name, batchSize, source, rewriter, m.store, m.proposer, m.builder, m.logger, m.meterProvider)
		w.Start()
		if !m.isCurrentLeader(generation) {
			w.Stop()

			return
		}

		m.workers[name] = w
	}

	m.logger.Infof("Mirror workers reconciled (active=%d)", len(m.workers))
}

// teardown stops all workers. Must be called under lock.
func (m *Manager) teardown() {
	for name, w := range m.workers {
		w.Stop()
		delete(m.workers, name)
	}
}

// createSource builds a Source from a MirrorSourceConfig oneof.
// todo: add pluggable source factory
func createSource(cfg *commonpb.MirrorSourceConfig) (v2.Source, error) {
	switch s := cfg.GetType().(type) {
	case *commonpb.MirrorSourceConfig_Http:
		var httpClient *http.Client
		if cc := s.Http.GetOauth2ClientCredentials(); cc != nil {
			httpClient = v2.NewOAuth2ClientCredentialsClient(cc.GetClientId(), cc.GetClientSecret(), cc.GetTokenEndpoint(), cc.GetScopes())
		}

		return v2.NewHTTPSource(s.Http.GetBaseUrl(), cfg.GetLedgerName(), httpClient), nil
	case *commonpb.MirrorSourceConfig_Postgres:
		return v2.NewPostgresSource(context.Background(), s.Postgres, cfg.GetLedgerName())
	default:
		return nil, fmt.Errorf("unsupported mirror source type: %T", s)
	}
}
