package replication

import (
	"context"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/pointer"
	"github.com/formancehq/go-libs/v4/query"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

// GlobalExporterStateStore abstracts the storage needed by the global exporter catch-up.
type GlobalExporterStateStore interface {
	Ledgers() common.PaginatedResource[ledger.Ledger, systemstore.ListLedgersQueryPayload]
	ListGlobalExporterStates(ctx context.Context) (map[string]uint64, error)
	UpdateGlobalExporterState(ctx context.Context, ledger string, lastLogID uint64) error
	DeleteAllGlobalExporterStates(ctx context.Context) error
}

var (
	DefaultGlobalExporterPollInterval           = 1 * time.Second
	DefaultGlobalExporterPushRetryPeriod        = 10 * time.Second
	DefaultGlobalExporterLogsPageSize           = uint64(100)
	DefaultGlobalExporterLedgerPollInterval     = 10 * time.Second
)

type globalExporterProgressTracker struct {
	ledgerName string
	lastLogID  *uint64
	store      GlobalExporterStateStore
	logger     logging.Logger
}

func (t *globalExporterProgressTracker) LedgerName() string {
	return t.ledgerName
}

func (t *globalExporterProgressTracker) LastLogID() *uint64 {
	return t.lastLogID
}

func (t *globalExporterProgressTracker) UpdateLastLogID(ctx context.Context, id uint64) error {
	t.lastLogID = &id
	if err := t.store.UpdateGlobalExporterState(ctx, t.ledgerName, id); err != nil {
		t.logger.Errorf("Failed to persist state for ledger %s: %v", t.ledgerName, err)
	}
	return nil
}

type GlobalExporterRunnerConfig struct {
	PollInterval       time.Duration
	PushRetryPeriod    time.Duration
	LogsPageSize       uint64
	LedgerPollInterval time.Duration
	Reset              bool
}

type GlobalExporterRunner struct {
	store         GlobalExporterStateStore
	rawDriver     drivers.Driver
	getLogFetcher func(ctx context.Context, name string) (LogFetcher, error)
	logger        logging.Logger
	config        GlobalExporterRunnerConfig
	stopOnce      sync.Once
	stopCh        chan struct{}
	doneCh        chan struct{}
}

func NewGlobalExporterRunner(
	store GlobalExporterStateStore,
	rawDriver drivers.Driver,
	openLedger func(ctx context.Context, name string) (LogFetcher, error),
	logger logging.Logger,
	config GlobalExporterRunnerConfig,
) *GlobalExporterRunner {
	if config.PollInterval == 0 {
		config.PollInterval = DefaultGlobalExporterPollInterval
	}
	if config.PushRetryPeriod == 0 {
		config.PushRetryPeriod = DefaultGlobalExporterPushRetryPeriod
	}
	if config.LogsPageSize == 0 {
		config.LogsPageSize = DefaultGlobalExporterLogsPageSize
	}
	if config.LedgerPollInterval == 0 {
		config.LedgerPollInterval = DefaultGlobalExporterLedgerPollInterval
	}

	var fetcherMu sync.Mutex
	fetcherCache := map[string]LogFetcher{}

	return &GlobalExporterRunner{
		store:     store,
		rawDriver: rawDriver,
		getLogFetcher: func(ctx context.Context, name string) (LogFetcher, error) {
			fetcherMu.Lock()
			defer fetcherMu.Unlock()
			if fetcher, ok := fetcherCache[name]; ok {
				return fetcher, nil
			}
			fetcher, err := openLedger(ctx, name)
			if err != nil {
				return nil, err
			}
			fetcherCache[name] = fetcher
			return fetcher, nil
		},
		logger: logger.WithField("component", "global-exporter"),
		config: config,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func (r *GlobalExporterRunner) Run(ctx context.Context) {
	defer close(r.doneCh)

	r.logger.Infof("Global exporter runner starting")

	if r.config.Reset {
		r.logger.Infof("Resetting global exporter state — all logs will be re-exported")
		if err := r.store.DeleteAllGlobalExporterStates(ctx); err != nil {
			panic("Failed to reset global exporter state")
		}
	}

	// Wrap driver in a facade for resilient startup
	facade := newDriverFacade(r.rawDriver, r.logger, r.config.PushRetryPeriod)
	facade.Run(ctx)

	// Ensure driver is always stopped on exit
	defer func() {
		if err := facade.Stop(ctx); err != nil {
			r.logger.Errorf("Error stopping driver: %v", err)
		}
		r.logger.Infof("Global exporter runner terminated")
	}()

	// Wait for driver to be ready or stop signal
	select {
	case <-facade.Ready():
		r.logger.Infof("Global exporter driver ready")
	case <-r.stopCh:
		return
	}

	// Load exporter states once from the DB; updated in-memory by trackers.
	states, err := r.store.ListGlobalExporterStates(ctx)
	if err != nil {
		r.logger.Errorf("Error loading initial global exporter states: %v", err)
		return
	}

	var (
		handlers        []*PipelineHandler
		wg              sync.WaitGroup
		lastSeenLedgerID int
	)

	defer func() {
		// Shut down all handlers on exit
		for _, handler := range handlers {
			if err := handler.Shutdown(ctx); err != nil {
				r.logger.Errorf("Error shutting down handler: %v", err)
			}
		}
		wg.Wait()
	}()

	pipelineOpts := []PipelineOption{
		WithPullPeriod(r.config.PollInterval),
		WithPushRetryPeriod(r.config.PushRetryPeriod),
		WithLogsPageSize(r.config.LogsPageSize),
	}

	nextInterval := time.Duration(0)

	for {
		select {
		case <-r.stopCh:
			return
		case <-time.After(nextInterval):
		}

		nextInterval = r.config.LedgerPollInterval

		// Discover new ledgers (only those with id > lastSeenLedgerID).
		var nextQuery common.PaginatedQuery[systemstore.ListLedgersQueryPayload] = common.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
			PageSize: 100,
			Column:   "id",
			Options: common.ResourceQuery[systemstore.ListLedgersQueryPayload]{
				Builder: query.Gt("id", lastSeenLedgerID),
			},
			Order: pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		}
		for {
			cursor, err := r.store.Ledgers().Paginate(ctx, nextQuery)
			if err != nil {
				r.logger.Errorf("Error listing ledgers: %v", err)
				break
			}

			for _, l := range cursor.Data {
				fetcher, err := r.getLogFetcher(ctx, l.Name)
				if err != nil {
					r.logger.Errorf("Error opening ledger %s: %v", l.Name, err)
					continue
				}

				var lastLogID *uint64
				if id, ok := states[l.Name]; ok {
					lastLogID = &id
				}

				tracker := &globalExporterProgressTracker{
					ledgerName: l.Name,
					lastLogID:  lastLogID,
					store:      r.store,
					logger:     r.logger,
				}

				handler := NewPipelineHandler(
					tracker,
					fetcher,
					facade,
					r.logger,
					pipelineOpts...,
				)
				handlers = append(handlers, handler)

				wg.Add(1)
				go func() {
					defer wg.Done()
					handler.Run(ctx)
				}()

				if l.ID > lastSeenLedgerID {
					lastSeenLedgerID = l.ID
				}
			}

			if !cursor.HasMore {
				break
			}
			nextQuery, err = common.UnmarshalCursor[systemstore.ListLedgersQueryPayload](cursor.Next)
			if err != nil {
				r.logger.Errorf("Error paginating ledgers: %v", err)
				break
			}
		}
	}
}

func (r *GlobalExporterRunner) Shutdown(ctx context.Context) error {
	r.logger.Infof("Shutting down global exporter runner")
	r.stopOnce.Do(func() { close(r.stopCh) })
	select {
	case <-r.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
