package mirror

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/go-libs/v4/logging"

	v2 "github.com/formancehq/ledger-v3-poc/internal/adapter/v2"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/preload"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Proposer submits a Raft proposal and returns a future for the apply result.
type Proposer interface {
	Propose(proposal *node.Proposal) (*futures.Future[state.ApplyResult], error)
}

// Manager manages the lifecycle of mirror workers based on the Raft-replicated
// ledger configuration. It creates one Worker per mirror ledger and only runs
// workers on the leader node.
type Manager struct {
	store         *dal.Store
	proposer      Proposer
	preloader     *preload.Preloader
	logger        logging.Logger
	notifications *signal.Notifications
	meterProvider metric.MeterProvider
	maxBatchSize  int

	mu       sync.Mutex
	isLeader bool
	workers  map[string]*Worker

	w worker.Worker
}

// NewManager creates a new mirror Manager.
func NewManager(store *dal.Store, proposer Proposer, preloader *preload.Preloader, logger logging.Logger, notifications *signal.Notifications, meterProvider metric.MeterProvider, maxBatchSize int) *Manager {
	return &Manager{
		store:         store,
		proposer:      proposer,
		preloader:     preloader,
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
			// Forward notification to all active workers
			for _, w := range m.workers {
				w.Notify()
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

// reconcile reads the current mirror ledger configurations from the store and
// starts or stops workers as needed. Must be called under lock.
func (m *Manager) reconcile() {
	if !m.isLeader {
		m.teardown()

		return
	}

	mirrorLedgers, err := query.ReadMirrorLedgers(context.Background(), m.store)
	if err != nil {
		m.logger.Errorf("Failed to load mirror ledgers: %v", err)

		return
	}

	// Build desired state as a set of ledger names
	desired := make(map[string]*commonpb.LedgerInfo, len(mirrorLedgers))
	for _, info := range mirrorLedgers {
		desired[info.GetName()] = info
	}

	// Remove workers for ledgers that are no longer mirrors
	for name, w := range m.workers {
		if _, stillDesired := desired[name]; !stillDesired {
			w.Stop()
			delete(m.workers, name)
		}
	}

	// Start workers for new mirror ledgers
	for name, info := range desired {
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

		batchSize := int(info.GetMirrorSource().GetBatchSize())
		if m.maxBatchSize > 0 && (batchSize <= 0 || batchSize > m.maxBatchSize) {
			batchSize = m.maxBatchSize
		}

		w := NewWorker(name, batchSize, source, m.store, m.proposer, m.preloader, m.logger, m.meterProvider)
		w.Start()
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
		return v2.NewPostgresSource(context.Background(), s.Postgres.GetDsn(), cfg.GetLedgerName())
	default:
		return nil, fmt.Errorf("unsupported mirror source type: %T", s)
	}
}
