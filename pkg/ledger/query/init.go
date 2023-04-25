package query

import (
	"context"

	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type InitLedger struct {
	cfg             *InitLedgerConfig
	driver          *storage.Driver
	monitor         monitor.Monitor
	metricsRegistry metrics.PerLedgerMetricsRegistry
}

type InitLedgerConfig struct {
	LimitReadLogs int
}

func NewInitLedgerConfig(limitReadLogs int) *InitLedgerConfig {
	return &InitLedgerConfig{
		LimitReadLogs: limitReadLogs,
	}
}

func NewDefaultInitLedgerConfig() *InitLedgerConfig {
	return &InitLedgerConfig{
		LimitReadLogs: 10000,
	}
}

func initLedgers(
	ctx context.Context,
	cfg *InitLedgerConfig,
	driver *storage.Driver,
	monitor monitor.Monitor,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) error {
	ledgers, err := driver.GetSystemStore().ListLedgers(ctx)
	if err != nil {
		return err
	}

	eg, ctxGroup := errgroup.WithContext(ctx)
	for _, ledger := range ledgers {
		_ledger := ledger
		eg.Go(func() error {
			store, err := driver.GetLedgerStore(ctxGroup, _ledger)
			if err != nil && !storageerrors.IsNotFoundError(err) {
				return err
			}

			if storageerrors.IsNotFoundError(err) {
				return nil
			}

			if !store.IsInitialized() {
				return nil
			}

			if _, err := initLedger(
				ctxGroup,
				cfg,
				_ledger,
				NewDefaultStore(store),
				monitor,
				metricsRegistry,
			); err != nil {
				return err
			}

			return nil
		})
	}

	return eg.Wait()
}

func initLedger(
	ctx context.Context,
	cfg *InitLedgerConfig,
	ledgerName string,
	store Store,
	monitor monitor.Monitor,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) (uint64, error) {
	if !store.IsInitialized() {
		return 0, nil
	}

	lastReadLogID, err := store.GetNextLogID(ctx)
	if err != nil && !storageerrors.IsNotFoundError(err) {
		return 0, errorsutil.NewError(ErrStorage,
			errors.Wrap(err, "reading last log"))
	}

	lastProcessedLogID := uint64(0)
	for {
		logs, err := store.ReadLogsRange(ctx, lastReadLogID, lastReadLogID+uint64(cfg.LimitReadLogs))
		if err != nil {
			return 0, errorsutil.NewError(ErrStorage,
				errors.Wrap(err, "reading logs since last ID"))
		}

		if len(logs) == 0 {
			// No logs, nothing to do
			return lastProcessedLogID, nil
		}

		if err := processLogs(ctx, ledgerName, store, monitor, logs...); err != nil {
			return 0, errors.Wrap(err, "processing logs")
		}

		metricsRegistry.QueryProcessedLogs().Add(ctx, int64(len(logs)))

		if err := store.UpdateNextLogID(ctx, logs[len(logs)-1].ID+1); err != nil {
			return 0, errorsutil.NewError(ErrStorage,
				errors.Wrap(err, "updating last read log"))
		}
		lastReadLogID = logs[len(logs)-1].ID + 1
		lastProcessedLogID = logs[len(logs)-1].ID

		if len(logs) < cfg.LimitReadLogs {
			// Nothing to do anymore, no need to read more logs
			return lastProcessedLogID, nil
		}

	}
}

func NewInitLedgers(cfg *InitLedgerConfig, driver *storage.Driver, monitor monitor.Monitor) *InitLedger {
	return &InitLedger{
		cfg:     cfg,
		driver:  driver,
		monitor: monitor,
	}
}
