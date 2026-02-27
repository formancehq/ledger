package replication

import (
	"context"
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
)

type GlobalExporterRunnerConfig struct {
	PollInterval    time.Duration
	PushRetryPeriod time.Duration
	LogsPageSize    uint64
	Reset           bool
}

type GlobalExporterRunner struct {
	store       GlobalExporterStateStore
	rawDriver   drivers.Driver
	openLedger  func(ctx context.Context, name string) (LogFetcher, error)
	logger      logging.Logger
	config      GlobalExporterRunnerConfig
	stopChannel chan chan error
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

	return &GlobalExporterRunner{
		store:       store,
		rawDriver:   rawDriver,
		openLedger:  openLedger,
		logger:      logger.WithField("component", "global-exporter"),
		config:      config,
		stopChannel: make(chan chan error, 1),
	}
}

func (r *GlobalExporterRunner) Run(ctx context.Context) {
	r.logger.Infof("Global exporter runner starting")

	if r.config.Reset {
		r.logger.Infof("Resetting global exporter state — all logs will be re-exported")
		if err := r.store.DeleteAllGlobalExporterStates(ctx); err != nil {
			r.logger.Errorf("Failed to reset global exporter state: %v", err)
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
	case ch := <-r.stopChannel:
		r.logger.Infof("Stop signal received before driver ready")
		close(ch)
		return
	}

	nextInterval := time.Duration(0)

	for {
		select {
		case ch := <-r.stopChannel:
			close(ch)
			return
		case <-time.After(nextInterval):
		}

		// Reset to default poll interval; exportLedgerLogs may set it to 0 if HasMore
		nextInterval = r.config.PollInterval

		states, err := r.store.ListGlobalExporterStates(ctx)
		if err != nil {
			r.logger.Errorf("Error listing global exporter states: %v", err)
			continue
		}

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
				lastLogID, hasState := states[l.Name]
				stopped := r.exportLedgerLogs(ctx, facade, l.Name, lastLogID, hasState)
				if stopped {
					return
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

// exportLedgerLogs fetches and pushes logs for a single ledger, retrying on
// failure. Returns true if a stop signal was received and the caller should exit.
func (r *GlobalExporterRunner) exportLedgerLogs(
	ctx context.Context,
	driver drivers.Driver,
	ledgerName string,
	lastLogID uint64,
	hasState bool,
) bool {
	fetcher, err := r.openLedger(ctx, ledgerName)
	if err != nil {
		r.logger.Errorf("Error opening ledger %s: %v", ledgerName, err)
		return false
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
			return false
		}

		if len(logs.Data) == 0 {
			return false
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
					case ch := <-r.stopChannel:
						r.logger.Infof("Global exporter runner terminated")
						close(ch)
						return true
					case <-time.After(r.config.PushRetryPeriod):
						continue
					}
				}
			case ch := <-r.stopChannel:
				cancel()
				r.logger.Infof("Global exporter runner terminated")
				close(ch)
				return true
			}

			break
		}

		// Persist state after successful batch
		lastLog := logs.Data[len(logs.Data)-1]
		if lastLog.ID != nil {
			if err := r.store.UpdateGlobalExporterState(ctx, ledgerName, *lastLog.ID); err != nil {
				r.logger.Errorf("Failed to persist state for ledger %s: %v", ledgerName, err)
			}
		}

		if !logs.HasMore {
			return false
		}

		// Check for stop signal before fetching next page
		select {
		case ch := <-r.stopChannel:
			r.logger.Infof("Global exporter runner terminated")
			close(ch)
			return true
		default:
		}

		logQuery, err = common.UnmarshalCursor[any](logs.Next)
		if err != nil {
			r.logger.Errorf("Error paginating logs for ledger %s: %v", ledgerName, err)
			return false
		}
	}
}

func (r *GlobalExporterRunner) Shutdown(ctx context.Context) error {
	r.logger.Infof("Shutting down global exporter runner")
	errorChannel := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.stopChannel <- errorChannel:
		r.logger.Debugf("Shutdown signal sent")
		select {
		case err := <-errorChannel:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
