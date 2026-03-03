package replication

import (
	"context"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/collectionutils"
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
	DefaultGlobalExporterPollInterval    = 1 * time.Second
	DefaultGlobalExporterPushRetryPeriod = 10 * time.Second
	DefaultGlobalExporterLogsPageSize    = uint64(100)
	DefaultGlobalExporterWorkers         = 8
)

type GlobalExporterRunnerConfig struct {
	PollInterval    time.Duration
	PushRetryPeriod time.Duration
	LogsPageSize    uint64
	Workers         int
	Reset           bool
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
	if config.Workers == 0 {
		config.Workers = DefaultGlobalExporterWorkers
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

	nextInterval := time.Duration(0)

	// Load exporter states once from the DB; updated in-memory after each successful export.
	states, err := r.store.ListGlobalExporterStates(ctx)
	if err != nil {
		r.logger.Errorf("Error loading initial global exporter states: %v", err)
		return
	}
	var statesMu sync.Mutex

	// workerCtx is cancelled when shutdown is requested, signalling workers to stop.
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for {
		select {
		case <-r.stopCh:
			return
		case <-time.After(nextInterval):
		}

		nextInterval = r.config.PollInterval

		// Collect all ledger names for this cycle.
		var ledgerNames []string
		var nextQuery common.PaginatedQuery[systemstore.ListLedgersQueryPayload] = common.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
			PageSize: 100,
			Column:   "id",
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		}
		for {
			cursor, err := r.store.Ledgers().Paginate(ctx, nextQuery)
			if err != nil {
				r.logger.Errorf("Error listing ledgers: %v", err)
				break
			}
			for _, l := range cursor.Data {
				ledgerNames = append(ledgerNames, l.Name)
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

		if len(ledgerNames) == 0 {
			continue
		}

		// Fan out to worker pool.
		work := make(chan string, len(ledgerNames))
		for _, name := range ledgerNames {
			work <- name
		}
		close(work)

		numWorkers := min(r.config.Workers, len(ledgerNames))

		workersDone := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(numWorkers)
		for range numWorkers {
			go func() {
				defer wg.Done()
				for name := range work {
					r.exportLedgerLogs(workerCtx, facade, name, &statesMu, states)
				}
			}()
		}
		go func() {
			wg.Wait()
			close(workersDone)
		}()

		// Wait for workers to finish or a stop signal.
		select {
		case <-workersDone:
		case <-r.stopCh:
			cancelWorkers()
			<-workersDone
			return
		}
	}
}

// exportLedgerLogs fetches and pushes logs for a single ledger, retrying on
// failure. Returns when the ledger is fully caught up or ctx is cancelled.
func (r *GlobalExporterRunner) exportLedgerLogs(
	ctx context.Context,
	driver drivers.Driver,
	ledgerName string,
	statesMu *sync.Mutex,
	states map[string]uint64,
) {
	statesMu.Lock()
	lastLogID, hasState := states[ledgerName]
	statesMu.Unlock()

	fetcher, err := r.getLogFetcher(ctx, ledgerName)
	if err != nil {
		r.logger.Errorf("Error opening ledger %s: %v", ledgerName, err)
		return
	}

	var builder query.Builder
	if hasState {
		builder = query.Gt("id", lastLogID)
	}

	var logQuery common.PaginatedQuery[any] = common.InitialPaginatedQuery[any]{
		PageSize: r.config.LogsPageSize,
		Column:   "id",
		Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		Options: common.ResourceQuery[any]{
			Builder: builder,
		},
	}

	for {
		logs, err := fetcher.ListLogs(ctx, logQuery)
		if err != nil {
			r.logger.Errorf("Error fetching logs for ledger %s: %v", ledgerName, err)
			return
		}

		if len(logs.Data) == 0 {
			return
		}

		// Push batch with retry
		for {
			r.logger.Debugf("Pushing %d logs for ledger %s", len(logs.Data), ledgerName)
			errChan := make(chan error, 1)
			exportCtx, cancel := context.WithCancel(ctx)
			go func() {
				_, err := driver.Accept(exportCtx, collectionutils.Map(logs.Data, func(log ledger.Log) drivers.LogWithLedger {
					return drivers.NewLogWithLedger(ledgerName, log)
				})...)
				errChan <- err
			}()

			select {
			case err := <-errChan:
				cancel()
				if err != nil {
					r.logger.Errorf("Error pushing logs for ledger %s: %v, retrying in %s", ledgerName, err, r.config.PushRetryPeriod)
					select {
					case <-ctx.Done():
						return
					case <-time.After(r.config.PushRetryPeriod):
						continue
					}
				}
			case <-ctx.Done():
				cancel()
				return
			}

			break
		}

		// Persist state after successful batch and update in-memory cache
		lastLogID := logs.Data[len(logs.Data)-1].ID
		if lastLogID != nil {
			if err := r.store.UpdateGlobalExporterState(ctx, ledgerName, *lastLogID); err != nil {
				r.logger.Errorf("Failed to persist state for ledger %s: %v", ledgerName, err)
			}
			statesMu.Lock()
			states[ledgerName] = *lastLogID
			statesMu.Unlock()
		}

		if !logs.HasMore {
			return
		}

		// Check for cancellation before fetching next page
		select {
		case <-ctx.Done():
			return
		default:
		}

		logQuery, err = common.UnmarshalCursor[any](logs.Next)
		if err != nil {
			r.logger.Errorf("Error paginating logs for ledger %s: %v", ledgerName, err)
			return
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
